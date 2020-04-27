package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/couchbase/vellum"
	"github.com/jbpratt78/imdb-index/internal/types"
)

func main() {

	/*
				dir := os.TempDir()
				if err := DownloadAll(dir); err != nil {
					log.Fatal(err)
				}
				defer os.RemoveAll(dir)
			if err := episode.Open("test-data"); err != nil {
				//if err := episodes(path.Join(dir, "data")); err != nil {
				if err == vellum.ErrIteratorDone {
					fmt.Println("Finished interating")
				} else {
					panic(err)
				}
			}
		idx, err := EpisodeOpen("index")
		if err != nil {
			panic(err)
		}
		eps, err := idx.Episodes([]byte("tt0096697"), 2)
		if err != nil {
			if err == vellum.ErrIteratorDone {
				fmt.Println("Finished interating")
			} else {
				panic(err)
			}
		}

		for _, e := range eps {
			fmt.Printf("%+v\n", e)
		}
	*/
	ratings()
}

func titles() {
	fstFile := path.Join("index", "title.fst")
	tsv, err := os.Open(path.Join("testdata", IMDBAKAS))
	if err != nil {
		panic(err)
	}
	defer tsv.Close()

	indexFile, err := os.Create(fstFile)
	if err != nil {
		panic(err)
	}

	var count uint64 = 0
	var buf bytes.Buffer
	var offset uint64
	type Record struct {
		id       string
		position uint64
		count    uint64
	}

	header := []string{}
	records := []Record{}

	tr := io.TeeReader(tsv, &buf)

	csvReader := csv.NewReader(tr)
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'

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

		line, err := buf.ReadBytes('\n')
		if err != nil {
			panic(err)
		}

		length := len(line)
		offset += uint64(length)

		records = append(records, Record{rec[0], offset, 1})
		// check count for priority and ++ if needed
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].id < records[j].id
	})

	builder, err := vellum.New(indexFile, nil)
	if err != nil {
		panic(err)
	}

	for _, record := range records {
		if err = builder.Insert([]byte(record.id), (record.count<<48)|record.position); err != nil {
			panic(err)
		}
		count += uint64(record.position)
	}

	if builder.Close(); err != nil {
		panic(err)
	}
	indexFile.Close()

	fmt.Printf("%d akas indexed\n", len(records))

	fst, err := vellum.Open(fstFile)
	if err != nil {
		panic(err)
	}
	defer fst.Close()

	fmt.Println(header)
	itr, err := fst.Iterator(nil, nil)
	for err == nil {
		key, val := itr.Current()
		if key == nil {
			break
		}

		id := binary.BigEndian.Uint64(key)

		count := id >> 48
		offset := val & ((1 << 48) - 1)

		fmt.Println(count, offset)
		ret, err := tsv.Seek(0, int(offset))
		if err != nil {
			panic(err)
		}
		fmt.Println(count, ret)
	}
}

func ratings() {
	fstRatingsFile := path.Join("index", "ratings.fst")
	tsv, err := os.Open(path.Join("testdata", IMDBRatings))
	if err != nil {
		panic(err)
	}
	defer tsv.Close()

	// create fst index file
	indexFile, err := os.Create(fstRatingsFile)
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

		record := &types.Rating{Id: rec[0], Rating: float32(rating), Votes: uint32(votes)}
		buffer = nil

		// write rating
		for _, b := range []byte(record.Id) {
			if b == 0 {
				panic(fmt.Errorf("unsupported rating id with nil byte for %v", rating))
			}
		}

		// append id
		for _, u := range []uint8(record.Id) {
			buffer = append(buffer, u)
		}

		buffer = append(buffer, 0x00)

		x := make([]byte, 4)
		binary.BigEndian.PutUint32(x, math.Float32bits(record.Rating))
		for _, u := range x {
			buffer = append(buffer, u)
		}

		y := make([]byte, 4)
		binary.BigEndian.PutUint32(y, record.Votes)
		for _, u := range y {
			buffer = append(buffer, u)
		}

		if err = builder.Insert(buffer, uint64(offset)); err != nil {
			panic(err)
		}

		count++
	}

	fmt.Println(count, "ratings indexed")
	if err = builder.Close(); err != nil {
		panic(err)
	}
	indexFile.Close()

	fst, err := vellum.Open(fstRatingsFile)
	if err != nil {
		panic(err)
	}
	defer fst.Close()

	fmt.Println(header)
	// id := []byte("tt0000019")
	// upper := append(id, 0xFF)
	// itr, err := fst.Iterator(id, upper)
	itr, err := fst.Iterator(nil, nil)
	for err == nil {
		nul := 0
		// don't care about val right now
		key, _ := itr.Current()
		// checking for nul byte to delimit id
		for i, b := range key {
			if b == 0x00 {
				nul = i
				break
			}
		}

		id := key[:nul]
		i := nul + 1
		rating := math.Float32frombits(binary.BigEndian.Uint32(key[i:]))
		votes := binary.BigEndian.Uint32(key[i+4:])
		fmt.Println(string(id), rating, votes)
		err = itr.Next()
	}
	if err != nil {
		panic(err)
	}
}
