package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"

	"github.com/couchbase/vellum"
)

func main() {
	tsv, err := os.Open("test-data/title.episode.tsv")
	if err != nil {
		panic(err)
	}
	defer tsv.Close()

	tvIndexFile, err := os.Create("index/episode.tvshows.fst")
	if err != nil {
		panic(err)
	}

	seasonIndexFile, err := os.Create("index/episode.seasons.fst")
	if err != nil {
		panic(err)
	}

	var buffer []uint8
	var episodes []Episode
	// make two builders
	header := []string{}

	csvReader := csv.NewReader(tsv)
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'

	// read sorted episodes
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

		season, _ := strconv.ParseUint(rec[2], 10, 32)
		episode, _ := strconv.ParseUint(rec[3], 10, 32)

		episodes = append(episodes, Episode{
			id:       rec[0],
			tvShowID: rec[1],
			season:   uint32(season),
			episode:  uint32(episode),
		})
	}

	sort.Slice(episodes, func(i, j int) bool {
		return episodes[i].tvShowID < episodes[j].tvShowID
	})

	seasonBuilder, err := vellum.New(seasonIndexFile, nil)
	if err != nil {
		panic(err)
	}

	for i, r := range episodes {
		for _, b := range []byte(r.tvShowID) {
			if b == 0 {
				panic(fmt.Errorf("unsupported rating id with nil byte for %v", r))
			}
		}

		for _, u := range []uint8(r.tvShowID) {
			buffer = append(buffer, u)
		}

		buffer = append(buffer, 0x00)

		if r.season != 0 {
			y := make([]byte, 4)
			binary.BigEndian.PutUint32(y, r.season)
			for _, u := range y {
				buffer = append(buffer, u)
			}
		}

		if r.episode != 0 {
			z := make([]byte, 4)
			binary.BigEndian.PutUint32(z, r.episode)
			for _, u := range z {
				buffer = append(buffer, u)
			}
		}

		for _, u := range []uint8(r.id) {
			buffer = append(buffer, u)
		}

		if err = seasonBuilder.Insert(buffer, uint64(i)); err != nil {
			panic(err)
		}
	}

	if seasonBuilder.Close(); err != nil {
		panic(err)
	}
	seasonIndexFile.Close()

	// loop over eps and write_ep, insert into tvshows builder

	_ = tvIndexFile
}

func ratings() {
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
		log.Fatal(err)
	}
}
