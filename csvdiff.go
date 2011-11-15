/*
The author disclaims copyright to this source code.
*/
package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/gwenn/yacr"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

type Keys []uint
type Row [][]byte
type Hasher hash.Hash64
type RowHash uint64
type Cache map[RowHash]Row

type Config struct {
	keys          Keys
	ignoredFields map[int]bool // TODO Set
	noHeader      bool
	sep           byte
	guess         bool
	quoted        bool
	format        int
	symbol        byte
	common        bool
}

/*
TODO Reduce memory allocation by reusing the same output buffer/row...
type Delta struct {
	values [][]byte
}
*/

func atouis(s string) (values []uint) {
	rawValues := strings.Split(s, ",")
	values = make([]uint, len(rawValues))
	for i, v := range rawValues {
		f, err := strconv.Atoui(v)
		if err != nil {
			flag.Usage()
			log.Fatalf("Invalid field index (%v)\n", v)
		}
		values[i] = f - 1
	}
	return
}

// TODO Add an option to ignore appended/new field(s).
func parseArgs() *Config {
	var n *bool = flag.Bool("n", false, "No header")
	var f *int = flag.Int("f", 0, "Format used to display delta (0: ansi bold, 1: piped, 2: newline)")
	var q *bool = flag.Bool("q", true, "Quoted field mode")
	var sep *string = flag.String("s", ",", "Set the field separator")
	var k *string = flag.String("k", "", "Set the key indexes (starts at 1)")
	var i *string = flag.String("i", "", "Set the ignored field indexes (starts at 1)")
	var c *bool = flag.Bool("c", false, "Output common/same lines")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-n] [-q] [-c] [-s=C] [-i=N,...] -k=N[,...] FILEA FILEB\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 2 {
		flag.Usage()
		log.Fatalf("Missing FILE argument(s)\n")
	} else if flag.NArg() > 2 {
		flag.Usage()
		log.Fatalf("Too many FILE arguments\n")
	}
	if *sep == "\\t" {
		*sep = "\t"
	} else if len(*sep) > 1 {
		flag.Usage()
		log.Fatalf("Separator must be only one character long\n")
	}
	guess := true
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "s" {
			guess = false
		}
	})

	var keys Keys
	if len(*k) > 0 {
		keys = atouis(*k)
	} else {
		flag.Usage()
		log.Fatalf("Missing Key argument(s)\n")
	}
	var ignoredFields = make(map[int]bool)
	if len(*i) > 0 {
		for _, index := range atouis(*i) {
			ignoredFields[int(index)] = true
		}
	}
	if *f == 0 {
		fi, e := os.Stdout.Stat()
		// Disable bold output when stdout is redirected to a file
		if e == nil && fi.IsRegular() {
			*f = 1
		}
	}
	var symbol byte
	if (*sep)[0] == '|' {
		symbol = '!'
	} else {
		symbol = '|'
	}
	return &Config{noHeader: *n, sep: (*sep)[0], guess: guess, quoted: *q,
		keys: keys, ignoredFields: ignoredFields, format: *f, symbol: symbol, common: *c}
}

func checkRow(rowA, rowB Row, config *Config) {
	for _, key := range config.keys {
		if int(key) >= len(rowA) || int(key) >= len(rowB) {
			log.Fatalf("Key index %d out of range\n", key+1)
		}
	}
	for field := range config.ignoredFields {
		if field >= len(rowA) || field >= len(rowB) {
			log.Fatalf("Ignored field %d out of range\n", field+1)
		}
	}
}

func hashRow(hasher Hasher, row Row, keys Keys) RowHash {
	hasher.Reset()
	for _, i := range keys {
		hasher.Write(row[i])
	}
	return RowHash(hasher.Sum64())
}

// May be introduce a Formatter
// TODO precision
func areEquals(rowA, rowB Row, config *Config, modifiedFields []bool) (rowDelta Row, same bool) {
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
		rowDelta = make(Row, maxLen+1) // TODO Reuse/cache one array and slice it?
		rowDelta[0] = []byte{'#'}
	}
	for i := 0; i < minLen; i++ {
		_, ignored := config.ignoredFields[i]
		// TODO skip keys
		if !ignored && !bytes.Equal(rowA[i], rowB[i]) {
			if same {
				rowDelta = make(Row, maxLen+1)
				rowDelta[0] = []byte{'#'}
				copy(rowDelta[1:], rowA[0:i])
			}
			same = false
			rowDelta[i+1] = concat(rowA[i], rowB[i], config.format, config.symbol)
			update(modifiedFields, i)
		} else if !same {
			rowDelta[i+1] = rowA[i]
		}
	}
	for i := minLen; i < maxLen; i++ {
		if _, ignored := config.ignoredFields[i]; ignored {
			continue
		}
		if longest == 1 {
			rowDelta[i+1] = concat(rowA[i], []byte{'_'}, config.format, config.symbol)
			update(modifiedFields, i)
		} else if longest == 2 {
			rowDelta[i+1] = concat([]byte{'_'}, rowB[i], config.format, config.symbol)
			update(modifiedFields, i)
		}
	}
	return
}

func update(modifiedFields []bool, i int) {
	if modifiedFields != nil && i < len(modifiedFields) {
		modifiedFields[i] = true
	}
}

func concat(valueA, valueB []byte, format int, symbol byte) []byte {
	switch format {
	case 1:
		return bytes.Join([][]byte{valueA, valueB}, []byte{symbol, '-', symbol})
	case 2:
		return bytes.Join([][]byte{valueA, valueB}, []byte{'\n'})
	}
	buf := []byte{}
	buf = append(buf, '\x1b', '[', '1', 'm')
	buf = append(buf, valueA...)
	buf = append(buf, '\x1b', '[', '0', 'm')
	buf = append(buf, symbol)
	buf = append(buf, '\x1b', '[', '1', 'm')
	buf = append(buf, valueB...)
	buf = append(buf, '\x1b', '[', '0', 'm')
	return buf
}

func delta(row Row, sign byte) (rowDelta Row) {
	rowDelta = make(Row, len(row)+1) // TODO Reuse/cache one array and slice it?
	rowDelta[0] = []byte{sign}
	copy(rowDelta[1:], row)
	return
}

func searchCache(cache Cache, key RowHash) (row Row, found bool, hash RowHash) {
	row, found = cache[key]
	if found {
		delete(cache, key)
		hash = key
	}
	return
}

func main() {
	config := parseArgs()

	readerA := makeReader(flag.Arg(0), config)
	defer readerA.Close()
	readerB := makeReader(flag.Arg(1), config)
	defer readerB.Close()

	cacheA := make(Cache)
	cacheB := make(Cache)

	hasher := fnv.New64a()
	writer := makeWriter(os.Stdout, config)

	var rowA, rowB, headers, rowDelta Row
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
		if first {
			checkRow(rowA, rowB, config)
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
			writer.MustWriteRow(delta(rowB, '+'))
			addedCount++
			continue
		}
		if rowB == nil {
			writer.MustWriteRow(delta(rowA, '-'))
			removedCount++
			continue
		}

		if hashA == hashB {
			if rowDelta, same = areEquals(rowA, rowB, config, modifiedFields); same {
				if first { // FIXME, Headers may be different (hashA != hashB)...
					first = false
					if !config.noHeader {
						writer.MustWriteRow(delta(rowA, '='))
						headers = deepCopy(rowA)
					} else if config.common {
						writer.MustWriteRow(delta(rowA, '='))
					}
					modifiedFields = make([]bool, len(rowA))
				} else if config.common {
					writer.MustWriteRow(delta(rowA, '='))
				}
			} else {
				writer.MustWriteRow(rowDelta)
				modifiedCount++
				if first {
					first = false
					if !config.noHeader {
						headers = deepCopy(rowDelta[1:])
					}
					modifiedFields = make([]bool, len(rowDelta)-1)
				}
			}
		} else {
			altB, found, _ := searchCache(cacheB, hashA)
			if found {
				if rowDelta, same = areEquals(rowA, altB, config, modifiedFields); !same {
					writer.MustWriteRow(rowDelta)
					modifiedCount++
				} else if config.common {
					writer.MustWriteRow(delta(rowA, '='))
				}
			} else {
				cacheA[hashA] = deepCopy(rowA)
			}
			altA, found, _ := searchCache(cacheA, hashB)
			if found {
				if rowDelta, same = areEquals(altA, rowB, config, modifiedFields); !same {
					writer.MustWriteRow(rowDelta)
					modifiedCount++
				} else if config.common {
					writer.MustWriteRow(delta(rowB, '='))
				}
			} else {
				cacheB[hashB] = deepCopy(rowB)
			}
		}
	}
	for _, rowA := range cacheA {
		writer.MustWriteRow(delta(rowA, '-'))
		removedCount++
	}
	for _, rowB := range cacheB {
		writer.MustWriteRow(delta(rowB, '+'))
		addedCount++
	}
	writer.MustFlush()
	if addedCount > 0 || removedCount > 0 || modifiedCount > 0 {
		fmt.Fprintf(os.Stderr, "Total: %d, Removed: %d, Added: %d, Modified: %d\n",
			totalCount, removedCount, addedCount, modifiedCount)
		if modifiedCount > 0 {
			fmt.Fprintf(os.Stderr, "Modified fields: ")
			modified := []string{}
			for i, b := range modifiedFields {
				if b {
					if headers != nil {
						modified = append(modified, fmt.Sprintf("%s (%d)", headers[i], i+1))
					} else {
						modified = append(modified, fmt.Sprintf("%d", i+1))
					}
				}
			}
			fmt.Fprintf(os.Stderr, "%s\n", strings.Join(modified, ", "))
		}
		os.Exit(1)
	}
}

func readRow(r *yacr.Reader, pEof bool) (row Row, eof bool) {
	if pEof {
		return nil, pEof
	}
	result, e := r.ReadRow()
	if e != nil {
		if e != io.EOF {
			log.Fatalf("Error while reading file: '%s'\n", e)
		}
		eof = true
	}
	row = result
	return
}

func makeReader(filepath string, c *Config) *yacr.Reader {
	reader, err := yacr.NewFileReader(filepath, c.sep, c.quoted)
	if err != nil {
		log.Fatalf("Error while opening file: '%s' (%s)\n", filepath, err)
	}
	reader.Guess = c.guess
	return reader
}
func makeWriter(wr io.Writer, c *Config) *yacr.Writer {
	writer := yacr.NewWriter(wr, c.sep, false /*TODO c.quoted */ )
	return writer
}

func deepCopy(row Row) Row {
	return yacr.DeepCopy(row)
}
