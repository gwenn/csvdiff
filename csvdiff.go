/*
The author disclaims copyright to this source code.
*/
package main

import (
	"bytes"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/gwenn/yacr"
)

type keys []int
type row [][]byte
type hasher hash.Hash64
type rowHash uint64
type cache map[rowHash]row

type config struct {
	keys          keys
	ignoredFields map[int]bool // TODO Set
	noHeader      bool
	sep           byte
	guess         bool
	quoted        bool
	format        int
	symbol        byte
	common        bool
	noTrailer     bool
}

/*
TODO Reduce memory allocation by reusing the same output buffer/row...
type Delta struct {
	values [][]byte
}
*/

func atouis(s string) (values []int) {
	rawValues := strings.Split(s, ",")
	values = make([]int, len(rawValues))
	for i, v := range rawValues {
		f, err := strconv.ParseInt(v, 10, 0)
		if err != nil || f < 1 {
			flag.Usage()
			log.Fatalf("Invalid field index (%v)\n", v)
		}
		values[i] = int(f) - 1
	}
	return
}

// TODO Add an option to ignore appended/new field(s).
func parseArgs() *config {
	var n = flag.Bool("n", false, "No header")
	var f = flag.Int("f", 0, "Format used to display delta (0: ansi bold, 1: piped, 2: newline)")
	var q = flag.Bool("q", true, "Quoted field mode")
	var sep = flag.String("s", ",", "Set the field separator")
	var k = flag.String("k", "", "Set the key indexes (starts at 1). '*' means all columns are part of the key")
	var i = flag.String("i", "", "Set the ignored field indexes (starts at 1)")
	var c = flag.Bool("c", false, "Output common/same lines")
	var t = flag.Bool("t", false, "No trailer")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-n] [-t] [-q] [-c] [-s=C] [-i=N,...] -k=N[,...] FILEA FILEB\n", os.Args[0])
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

	var keys keys
	if len(*k) > 0 {
		if "*" != *k {
			keys = atouis(*k)
		}
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
		if e == nil && (fi.Mode()&os.ModeType == 0) {
			*f = 1
		}
	}
	var symbol byte
	if (*sep)[0] == '|' {
		symbol = '!'
	} else {
		symbol = '|'
	}
	return &config{noHeader: *n, sep: (*sep)[0], guess: guess, quoted: *q,
		keys: keys, ignoredFields: ignoredFields, format: *f, symbol: symbol, common: *c, noTrailer: *t}
}

func checkRow(rowA, rowB row, config *config) {
	if len(config.keys) == 0 { // when no key is specified, take all columns...
		var n int
		if len(rowA) < len(rowB) {
			n = len(rowA)
		} else {
			n = len(rowB)
		}
		config.keys = make([]int, n)
		for i := range config.keys {
			config.keys[i] = i
		}
	}
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

func hashRow(hasher hasher, r row, keys keys) rowHash {
	hasher.Reset()
	for _, i := range keys {
		hasher.Write(r[i])
	}
	return rowHash(hasher.Sum64())
}

// May be introduce a Formatter
// TODO precision
func areEquals(rowA, rowB row, config *config, modifiedFields []bool) (rowDelta row, same bool) {
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
		rowDelta = make(row, maxLen+1) // TODO Reuse/cache one array and slice it?
		rowDelta[0] = []byte{'#'}
	}
	for i := 0; i < minLen; i++ {
		_, ignored := config.ignoredFields[i]
		// TODO skip keys
		if !ignored && !bytes.Equal(rowA[i], rowB[i]) {
			if same {
				rowDelta = make(row, maxLen+1)
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
	case 3:
		return bytes.Join([][]byte{valueA, valueB}, []byte{symbol})

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

func delta(r row, sign byte) (rowDelta row) {
	rowDelta = make(row, len(r)+1) // TODO Reuse/cache one array and slice it?
	rowDelta[0] = []byte{sign}
	copy(rowDelta[1:], r)
	return
}

func searchCache(cache cache, key rowHash) (r row, found bool, hash rowHash) {
	r, found = cache[key]
	if found {
		delete(cache, key)
		hash = key
	}
	return
}

func main() {
	config := parseArgs()

	readerA, inA := makeReader(flag.Arg(0), config)
	defer inA.Close()
	readerB, inB := makeReader(flag.Arg(1), config)
	defer inB.Close()

	cacheA := make(cache)
	cacheB := make(cache)

	hasher := fnv.New64a()
	writer := makeWriter(os.Stdout, config)

	var bufferA, bufferB row = make([][]byte, 0, 10), make([][]byte, 0, 10)
	var rowA, rowB, headers, rowDelta row
	var hashA, hashB rowHash
	var addedCount, modifiedCount, removedCount, totalCount uint
	var eofA, eofB, same bool
	var modifiedFields []bool
	first := true
	for !eofA || !eofB {
		bufferA, rowA, eofA = readRow(readerA, bufferA, eofA)
		bufferB, rowB, eofB = readRow(readerB, bufferB, eofB)
		if rowA == nil && rowB == nil {
			continue
		}
		if first {
			checkRow(rowA, rowB, config)
			/*if config.guess {
				writer.Sep = readerA.Sep
				if writer.Sep == '|' {
					config.symbol = '!'
				}
			}*/
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
			writeRow(writer, delta(rowB, '+'))
			addedCount++
			continue
		}
		if rowB == nil {
			writeRow(writer, delta(rowA, '-'))
			removedCount++
			continue
		}

		if hashA == hashB {
			if rowDelta, same = areEquals(rowA, rowB, config, modifiedFields); same {
				if first { // FIXME, Headers may be different (hashA != hashB)...
					first = false
					if !config.noHeader {
						writeRow(writer, delta(rowA, '='))
						headers = deepCopy(rowA)
					} else if config.common {
						writeRow(writer, delta(rowA, '='))
					}
					modifiedFields = make([]bool, len(rowA))
				} else if config.common {
					writeRow(writer, delta(rowA, '='))
				}
			} else {
				writeRow(writer, rowDelta)
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
					writeRow(writer, rowDelta)
					modifiedCount++
				} else if config.common {
					writeRow(writer, delta(rowA, '='))
				}
			} else {
				if _, exist := cacheA[hashA]; exist {
					fmt.Fprintf(os.Stderr, "Duplicate row/key in first input: %s\n", rowA)
				}
				cacheA[hashA] = deepCopy(rowA)
			}
			altA, found, _ := searchCache(cacheA, hashB)
			if found {
				if rowDelta, same = areEquals(altA, rowB, config, modifiedFields); !same {
					writeRow(writer, rowDelta)
					modifiedCount++
				} else if config.common {
					writeRow(writer, delta(rowB, '='))
				}
			} else {
				if _, exist := cacheB[hashB]; exist {
					fmt.Fprintf(os.Stderr, "Duplicate row/key in second input: %s\n", rowB)
				}
				cacheB[hashB] = deepCopy(rowB)
			}
		}
	}
	for _, rowA := range cacheA {
		writeRow(writer, delta(rowA, '-'))
		removedCount++
	}
	for _, rowB := range cacheB {
		writeRow(writer, delta(rowB, '+'))
		addedCount++
	}
	writer.Flush()
	if err := writer.Err(); err != nil {
		log.Fatalf("Error while flushing diff: '%s'\n", err)
	}
	if addedCount > 0 || removedCount > 0 || modifiedCount > 0 {
		if !config.noTrailer {
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
		}
		os.Exit(1)
	}
}

func readRow(r *yacr.Reader, buffer row, pEof bool) (row, row, bool) {
	if pEof {
		return buffer, nil, pEof
	}
	var eof bool
	var v, cv []byte
	orig := buffer
	i := 0
	buffer = buffer[:0]
	for {
		if r.Scan() {
			v = r.Bytes() // must be copied
			if i < len(orig) {
				cv = orig[i]
				cv = append(cv[:0], v...)
			} else {
				cv = make([]byte, len(v))
				copy(cv, v)
			}
			buffer = append(buffer, cv)
			if r.EndOfRecord() {
				break
			}
		} else {
			eof = true
			break
		}
		i++
	}
	if err := r.Err(); err != nil {
		log.Fatalf("Error while reading file: '%s'\n", err)
	}
	if len(buffer) == 0 {
		return buffer, nil, eof
	}
	return buffer, buffer, eof
}

func writeRow(w *yacr.Writer, r row) {
	for _, field := range r {
		w.Write(field)
	}
	w.EndOfRecord()
	if err := w.Err(); err != nil {
		log.Fatalf("Error while writing diff: '%s'\n", err)
	}
}

func makeReader(filepath string, c *config) (*yacr.Reader, io.ReadCloser) {
	in, err := yacr.Zopen(filepath)
	if err != nil {
		log.Fatalf("Error while opening file: '%s' (%s)\n", filepath, err)
	}
	reader := yacr.NewReader(in, c.sep, c.quoted, c.guess)
	return reader, in
}
func makeWriter(wr io.Writer, c *config) *yacr.Writer {
	writer := yacr.NewWriter(wr, c.sep, false /*TODO c.quoted */)
	return writer
}

func deepCopy(r row) row {
	dup := make(row, len(r))
	for i := 0; i < len(r); i++ {
		dup[i] = make([]byte, len(r[i]))
		copy(dup[i], r[i])
	}
	return dup
}
