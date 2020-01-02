package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	"github.com/couchbase/vellum"
)

func main() {
	tsv, err := os.Open("test-data/title.ratings.tsv")
	if err != nil {
		panic(err)
	}
	defer tsv.Close()

	// create fst index file
	indexFile, err := os.Create("index/ratings.fst")
	if err != nil {
		panic(err)
	}

	var count uint64 = 0
	var buf bytes.Buffer
	var offset int64
	var buffer []uint8

	header := []string{}

	tr := io.TeeReader(tsv, &buf)

	csvReader := csv.NewReader(tr)
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'

	builder, err := vellum.New(indexFile, nil)
	if err != nil {
		panic(err)
	}

	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		if len(header) == 0 {
			header = rec
			continue
		}

		// get offset
		line, err := buf.ReadBytes('\n')
		if err != nil {
			panic(err)
		}

		length := len(line)
		offset += int64(length)

		rating, err := strconv.ParseFloat(rec[1], 32)
		if err != nil {
			panic(err)
		}

		votes, err := strconv.ParseUint(rec[2], 10, 32)
		if err != nil {
			panic(err)
		}

		record := &Rating{id: rec[0], rating: float32(rating), votes: uint32(votes)}
		buffer = nil

		// write rating {
		for _, b := range []byte(record.id) {
			if b == 0 {
				panic(fmt.Errorf("unsupported rating id with nil byte for %v", rating))
			}
		}

		// append id
		for _, u := range []uint8(record.id) {
			buffer = append(buffer, u)
		}

		buffer = append(buffer, 0x00)

		x := make([]byte, 4)
		binary.BigEndian.PutUint32(x, math.Float32bits(record.rating))
		for _, u := range x {
			buffer = append(buffer, u)
		}

		y := make([]byte, 4)
		binary.BigEndian.PutUint32(y, record.votes)
		for _, u := range y {
			buffer = append(buffer, u)
		}
		// }

		if err = builder.Insert(buffer, uint64(offset)); err != nil {
			panic(err)
		}

		count += 1
	}

	fmt.Println(count, "ratings indexed")
	if err = builder.Close(); err != nil {
		panic(err)
	}
	indexFile.Close()

	fst, err := vellum.Open("index/ratings.fst")
	if err != nil {
		panic(err)
	}
	defer fst.Close()

	id := []byte("tt0000019")
	upper := append(id, 0xFF)
	itr, err := fst.Iterator(id, upper)
	for err == nil {
		key, val := itr.Current()
		fmt.Printf("contains key: %s val: %d\n", key, val)
		err = itr.Next()
	}
	if err != nil {
		panic(err)
	}
}
