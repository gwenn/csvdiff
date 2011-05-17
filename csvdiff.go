/*
The author disclaims copyright to this source code.
*/
package main

import (
	"bufio"
	"compress/gzip"
	"gocsv.googlecode.com/hg"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"strconv"
)

type Keys []uint
type Row []string
type Hasher hash.Hash64
type RowHash uint64
type Cache map[RowHash]Row

type Config struct {
	keys      Keys
	ignoredFields map[int]bool // TODO Set
	noHeader  bool
	separator byte
	bold  bool
}

func atouis(s string) (values []uint) {
	rawValues := strings.Split(s, ",", -1)
	values = make([]uint, len(rawValues))
	for i, v := range rawValues {
		f, err := strconv.Atoui(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid field index (%v)\n", v)
			flag.Usage()
			os.Exit(1)
		}
		values[i] = f - 1
	}
	return
}

func parseArgs() *Config {
	var n *bool = flag.Bool("n", false, "No header")
	var b *bool = flag.Bool("b", false, "Display delta in bold (ansi)")
	var sep *string = flag.String("s", ";", "Set the field separator")
	var k *string = flag.String("k", "", "Set the key indexes (starts at 1)")
	var i *string = flag.String("i", "", "Set the ignored field indexes (starts at 1)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-nb] [-s=C] [-i=N,...] -k=N[,...] FILEA FILEB\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "Missing FILE argument(s)\n")
		flag.Usage()
		os.Exit(1)
	} else if flag.NArg() > 2 {
		fmt.Fprintf(os.Stderr, "Too many FILE arguments\n")
		flag.Usage()
		os.Exit(1)
	}
	if len(*sep) > 1 {
		fmt.Fprintf(os.Stderr, "Separator must be only one character long\n")
		flag.Usage()
		os.Exit(1)
	}
	var keys Keys
	if len(*k) > 0 {
		keys = atouis(*k)
	} else {
		fmt.Fprintf(os.Stderr, "Missing Key argument(s)\n")
				flag.Usage()
				os.Exit(1)
			}
	var ignoredFields = make(map[int]bool)
	if len(*i) > 0 {
		for _, index := range atouis(*i) {
			ignoredFields[int(index)] = true
		}
	}
	if *b {
		fi, e := os.Stdout.Stat()
		// Disable bold output when stdout is redirected to a file
		if e == nil && fi.IsRegular() {
			*b = false
		}
	}
	return &Config{noHeader: *n, separator: (*sep)[0], keys: keys, ignoredFields: ignoredFields, bold: *b}
}

func hashRow(hasher Hasher, row Row, keys Keys) RowHash {
	hasher.Reset()
	for _, i := range keys {
		hasher.Write([]byte(row[i]))
	}
	return RowHash(hasher.Sum64())
}

// May be introduce a Formatter
// TODO precision
// TODO stats on modified fields
func areEquals(rowA, rowB Row, ignoredFields map[int]bool, modifiedFields []bool, bold bool) (rowDelta Row, same bool) {
	same = true
	var minLen, maxLen, longest int
	if len(rowA) > len(rowB) {
		maxLen = len(rowA)
		minLen = len(rowB)
		longest = 1
		same = false
	} else {
		maxLen = len(rowB)
		minLen = len(rowA)
		if maxLen > minLen {
			longest = 2
			same = false
		}
	}
	if !same {
		rowDelta = make(Row, maxLen+1)
		rowDelta[0] = "#"
	}
	for i := 0; i < minLen; i++ {
		_, ignored := ignoredFields[i]
		// TODO skip keys
		if !ignored && rowA[i] != rowB[i] {
			if same {
				rowDelta = make(Row, maxLen+1)
				rowDelta[0] = "#"
				for j := 0; j < i; j++ {
					rowDelta[j+1] = rowA[j]
				}
			}
			same = false
			rowDelta[i+1] = concat(rowA[i], rowB[i], bold)
			update(modifiedFields, i)
		} else if !same {
			rowDelta[i+1] = rowA[i]
		}
	}
	for i := minLen; i < maxLen; i++ {
		if longest == 1 {
			rowDelta[i+1] = concat(rowA[i], "_", bold)
			update(modifiedFields, i)
		} else if longest == 2 {
			rowDelta[i+1] = concat("_", rowB[i], bold)
			update(modifiedFields, i)
		}
	}
	return
}

func update(modifiedFields []bool, i int) {
	if modifiedFields != nil {
		modifiedFields[i] = true
	}
}

func concat(valueA, valueB string, bold bool) string {
	if bold {
		return "\x1b[1m" + valueA + "\x1b[0m|\x1b[1m" + valueB + "\x1b[0m"
	}
	return valueA + "\n" + valueB
}

func delta(row Row, sign string) (rowDelta Row) {
	rowDelta = make(Row, len(row)+1)
	rowDelta[0] = sign
	for i, v := range row {
		rowDelta[i+1] = v
	}
	return
}

func searchCache(cache Cache, key RowHash) (row Row, found bool, hash RowHash) {
	row, found = cache[key]
	if found {
		cache[key] = nil, false
		hash = key
	}
	return
}

func main() {
	config := parseArgs()

	// TODO Create golang bindings for zlib (gzopen)
	fileA := open(flag.Arg(0))
	defer fileA.Close()
	decompA := decomp(fileA)
	defer decompA.Close()
	fileB := open(flag.Arg(1))
	defer fileB.Close()
	decompB := decomp(fileB)
	defer decompB.Close()

	// TODO Optimized CSV Reader with only one allocated array/slice by file
	readerA := makeReader(decompA, config.separator)
	readerB := makeReader(decompB, config.separator)

	cacheA := make(Cache)
	cacheB := make(Cache)

	hasher := fnv.New64a()
	writer := makeWriter(os.Stdout, config.separator)

	var rowA, rowB, header, rowDelta Row
	var hashA, hashB RowHash
	var addedCount, modifiedCount, removedCount, totalCount uint
	var eofA, eofB, same bool
	var modifiedFields []bool
	first := true
	for !eofA || !eofB {
		rowA, eofA = readRow(readerA, eofA)
		rowB, eofB = readRow(readerB, eofB)
		if rowA == nil && rowB == nil {
			continue
		}
		totalCount++
		if rowA != nil && rowB != nil {
			hashA = hashRow(hasher, rowA, config.keys)
			hashB = hashRow(hasher, rowB, config.keys)
		} else if rowA != nil {
			hashA = hashRow(hasher, rowA, config.keys)
			rowB, _, hashB = searchCache(cacheB, hashA)
		} else if rowB != nil {
			hashB = hashRow(hasher, rowB, config.keys)
			rowA, _, hashA = searchCache(cacheA, hashB)
		}

		if rowA == nil {
			writer.WriteRow(delta(rowB, "+"))
			addedCount++
			continue
		}
		if rowB == nil {
			writer.WriteRow(delta(rowA, "-"))
			removedCount++
			continue
		}

		if hashA == hashB {
			if rowDelta, same = areEquals(rowA, rowB, config.ignoredFields, modifiedFields, config.bold); same {
				if first {
					first = false
					if !config.noHeader {
						writer.WriteRow(delta(rowA, "="))
					}
					header = make(Row, len(rowA))
					copy(header, rowA)
					modifiedFields = make([]bool, len(rowA))
				}
			} else {
				writer.WriteRow(rowDelta)
				modifiedCount++
				if first {
					first = false
					header = make(Row, len(rowDelta) -1)
					copy(header, rowDelta[1:])
					modifiedFields = make([]bool, len(rowDelta) - 1)
				}
			}
		} else {
			altB, found, _ := searchCache(cacheB, hashA)
			if found {
				if rowDelta, same = areEquals(rowA, altB, config.ignoredFields, modifiedFields, config.bold); !same {
					writer.WriteRow(rowDelta)
					modifiedCount++
				}
			} else {
				cacheA[hashA] = rowA
			}
			altA, found, _ := searchCache(cacheA, hashB)
			if found {
				if rowDelta, same = areEquals(altA, rowB, config.ignoredFields, modifiedFields, config.bold); !same {
					writer.WriteRow(rowDelta)
					modifiedCount++
				}
			} else {
				cacheB[hashB] = rowB
			}
		}
	}
	for _, rowA := range cacheA {
		writer.WriteRow(delta(rowA, "-"))
		removedCount++
	}
	for _, rowB := range cacheB {
		writer.WriteRow(delta(rowB, "+"))
		addedCount++
	}
	if addedCount > 0 || removedCount > 0 || modifiedCount > 0 {
		fmt.Fprintf(os.Stderr, "Total: %d, Removed: %d, Added: %d, Modified: %d\n",
			totalCount, removedCount, addedCount, modifiedCount)
		fmt.Fprintf(os.Stderr, "Modified fields: ")
		for i, b := range modifiedFields {
			if b {
				if header != nil {
					fmt.Fprintf(os.Stderr, "%s, ", header[i])
				} else {
					fmt.Fprintf(os.Stderr, "%d, ", i + 1)
				}
			}
		}
		fmt.Fprintf(os.Stderr, "\n")
		os.Exit(1)
	}
}

func readRow(r *csv.Reader, pEof bool) (row Row, eof bool) {
	if pEof {
		return nil, pEof
	}
	result, e := r.ReadRow()
	if e != nil {
		if e != os.EOF {
			fmt.Fprintf(os.Stderr, "Error while reading file: '%s'\n", e)
			os.Exit(1)
		}
		eof = true
	}
	row = result
	return
}

func makeReader(rd io.Reader, sep byte) *csv.Reader {
	bufIn := bufio.NewReader(rd)
	reader := csv.NewReader(bufIn)
	reader.Config.FieldDelim = sep
	return reader
}
func makeWriter(wr io.Writer, sep byte) *csv.Writer {
	bufOut := bufio.NewWriter(wr)
	writer := csv.NewWriter(bufOut)
	writer.Config.FieldDelim = sep
	return writer
}

func open(filePath string) *os.File {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while opening file: '%s' (%s)\n", filePath, err)
		os.Exit(1)
	}
	return file
}

func decomp(f *os.File) (r io.ReadCloser) {
	var err os.Error
	if strings.HasSuffix(f.Name(), ".gz") {
		r, err = gzip.NewReader(f)
	} else {
		r = f
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while decompressing file: '%s' (%s)\n", f, err)
		os.Exit(1)
	}
	return r
}
