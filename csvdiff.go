/*
The author disclaims copyright to this source code.
*/
package main

import (
	"bufio"
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
	noHeader  bool
	separator byte
	bold  bool
}

// TODO [ignored field(s)]
func parseArgs() *Config {
	var n *bool = flag.Bool("n", false, "No header")
	var b *bool = flag.Bool("b", false, "Display delta in bold (ansi)")
	var sep *string = flag.String("s", ";", "Set the field separator")
	var k *string = flag.String("k", "", "Set the key indexes (starts at 1)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-nb] [-s=C] -k=N[,...] FILEA FILEB\n", os.Args[0])
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
		rawKeys := strings.Split(*k, ",", -1)
		keys = make(Keys, len(rawKeys))
		for i, s := range rawKeys {
			f, err := strconv.Atoui(s)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid field index (%v)\n", s)
				flag.Usage()
				os.Exit(1)
			}
			keys[i] = f - 1
		}
	} else {
		fmt.Fprintf(os.Stderr, "Missing FILE argument(s)\n")
		flag.Usage()
		os.Exit(1)
	}
	return &Config{noHeader: *n, separator: (*sep)[0], keys: keys, bold: *b}
}

func hashRow(hasher Hasher, row Row, keys Keys) RowHash {
	hasher.Reset()
	for _, i := range keys {
		hasher.Write([]byte(row[i]))
	}
	return (RowHash)(hasher.Sum64())
}

// May be introduce a Formatter
// TODO ignored field(s)
// TODO precision
// TODO stats on modified fields
func areEquals(rowA, rowB Row, modifiedFields []bool, bold bool) (rowDelta Row, same bool) {
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
		// TODO skip keys
		if rowA[i] != rowB[i] {
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

	fileA := open(flag.Arg(0))
	defer fileA.Close()
	fileB := open(flag.Arg(1))
	defer fileB.Close()

	// FIXME "compress/gzip"
	// FIXME "compress/bzip2"

	// TODO Optimized CSV Reader with only one allocated array/slice by file
	readerA := makeReader(fileA, config.separator)
	readerB := makeReader(fileB, config.separator)

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
		if rowA != nil {
			hashA = hashRow(hasher, rowA, config.keys)
		} else {
			rowA, _, hashA = searchCache(cacheA, hashB)
		}
		if rowB != nil {
			hashB = hashRow(hasher, rowB, config.keys)
		} else {
			rowB, _, hashB = searchCache(cacheB, hashA)
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
			if rowDelta, same = areEquals(rowA, rowB, modifiedFields, config.bold); same {
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
				if rowDelta, same = areEquals(rowA, altB, modifiedFields, config.bold); !same {
					writer.WriteRow(rowDelta)
					modifiedCount++
				}
			} else {
				cacheA[hashA] = rowA
			}
			altA, found, _ := searchCache(cacheA, hashB)
			if found {
				if rowDelta, same = areEquals(altA, rowB, modifiedFields, config.bold); !same {
					writer.WriteRow(rowDelta)
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
