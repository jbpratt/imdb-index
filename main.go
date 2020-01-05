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
		if episodes[i].tvShowID != episodes[j].tvShowID {
			return episodes[i].tvShowID < episodes[j].tvShowID
		}
		if episodes[i].season != episodes[j].season {
			return episodes[i].season < episodes[j].season
		}
		if episodes[i].episode != episodes[j].episode {
			return episodes[i].episode < episodes[j].episode
		}
		return episodes[i].id < episodes[j].id
	})

	seasonBuilder, err := vellum.New(seasonIndexFile, nil)
	if err != nil {
		panic(err)
	}

	for i, ep := range episodes {
		buffer = nil
		for _, b := range []byte(ep.tvShowID) {
			if b == 0 {
				panic(fmt.Errorf("unsupported rating id with nil byte for %v", ep))
			}
		}

		for _, u := range []uint8(ep.tvShowID) {
			buffer = append(buffer, u)
		}

		buffer = append(buffer, 0x00)

		// fn extend_from_slice(&u32_to_bytes(to_optional_season(ep)?)) {
		if ep.season != 0 {
			if ep.season == ^uint32(0) {
				panic(fmt.Errorf("unsupported season number %d for %v", ep.season, ep))
			}
			y := make([]byte, 4)
			binary.BigEndian.PutUint32(y, ep.season)
			for _, u := range y {
				buffer = append(buffer, u)
			}
		} else {
			// uint32 max
			z := make([]byte, 4)
			binary.BigEndian.PutUint32(z, ^uint32(0))
			for _, u := range z {
				buffer = append(buffer, u)
			}
		}
		// }

		// fn extend_from_slice(&u32_to_bytes(to_optional_epnum(ep)?)) {
		if ep.episode != 0 {
			if ep.episode == ^uint32(0) {
				panic(fmt.Errorf("unsupported season number %d for %v", ep.episode, ep))
			}
			z := make([]byte, 4)
			binary.BigEndian.PutUint32(z, ep.episode)
			for _, u := range z {
				buffer = append(buffer, u)
			}
		} else {
			// uint32 max
			z := make([]byte, 4)
			binary.BigEndian.PutUint32(z, ^uint32(0))
			for _, u := range z {
				buffer = append(buffer, u)
			}
		}
		// }

		for _, u := range []uint8(ep.id) {
			buffer = append(buffer, u)
		}

		if err = seasonBuilder.Insert(buffer, uint64(i)); err != nil {
			panic(err)
		}
	}

	if err = seasonBuilder.Close(); err != nil {
		panic(err)
	}
	seasonIndexFile.Close()

	tvBuilder, err := vellum.New(tvIndexFile, nil)
	if err != nil {
		panic(err)
	}

	sort.Slice(episodes, func(i, j int) bool {
		if episodes[i].id != episodes[j].id {
			return episodes[i].id < episodes[j].id
		}
		return episodes[i].tvShowID < episodes[j].tvShowID
	})

	for i, ep := range episodes {
		buffer = nil
		for _, b := range []byte(ep.id) {
			if b == 0 {
				panic(fmt.Errorf("unsupported rating id with nil byte for %v", ep))
			}
		}

		for _, u := range []uint8(ep.id) {
			buffer = append(buffer, u)
		}

		buffer = append(buffer, 0x00)

		// fn extend_from_slice(&u32_to_bytes(to_optional_season(ep)?)) {
		if ep.season != 0 {
			if ep.season == ^uint32(0) {
				panic(fmt.Errorf("unsupported season number %d for %v", ep.season, ep))
			}
			y := make([]byte, 4)
			binary.BigEndian.PutUint32(y, ep.season)
			for _, u := range y {
				buffer = append(buffer, u)
			}
		} else {
			// uint32 max
			z := make([]byte, 4)
			binary.BigEndian.PutUint32(z, ^uint32(0))
			for _, u := range z {
				buffer = append(buffer, u)
			}
		} // }

		// fn extend_from_slice(&u32_to_bytes(to_optional_epnum(ep)?)) {
		if ep.episode != 0 {
			if ep.episode == ^uint32(0) {
				panic(fmt.Errorf("unsupported season number %d for %v", ep.episode, ep))
			}
			z := make([]byte, 4)
			binary.BigEndian.PutUint32(z, ep.episode)
			for _, u := range z {
				buffer = append(buffer, u)
			}
		} else {
			// uint32 max
			z := make([]byte, 4)
			binary.BigEndian.PutUint32(z, ^uint32(0))
			for _, u := range z {
				buffer = append(buffer, u)
			}
		} // }

		for _, u := range []uint8(ep.tvShowID) {
			buffer = append(buffer, u)
		}

		fmt.Println(i, buffer)
		if err = tvBuilder.Insert(buffer, uint64(i)); err != nil {
			panic(err)
		}
	}

	if tvBuilder.Close(); err != nil {
		panic(err)
	}
	tvIndexFile.Close()

	fmt.Printf("%d episodes indexed\n", len(episodes))

	fmt.Println("reading seasons index")
	seasonsFst, err := vellum.Open("index/episode.seasons.fst")
	if err != nil {
		panic(err)
	}
	defer seasonsFst.Close()
	fmt.Println(header)
	itr, err := seasonsFst.Iterator(nil, nil)
	for err == nil {
		nul := 0
		key, _ := itr.Current()
		for i, b := range key {
			if b == 0x00 {
				nul = i
				break
			}
		}

		tvShowID := key[:nul]
		i := nul + 1

		season := binary.BigEndian.Uint32(key[i:])

		i += 4
		epnum := binary.BigEndian.Uint32(key[i:])

		i += 4
		id := key[i:]

		ep := &Episode{
			id:       string(id),
			tvShowID: string(tvShowID),
			season:   season,
			episode:  epnum,
		}

		fmt.Println(ep)
		err = itr.Next()
	}
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("reading tvshows index")
	tvshowsFst, err := vellum.Open("index/episodes.tvshows.fst")
	if err != nil {
		panic(err)
	}
	defer tvshowsFst.Close()

	itr, err = tvshowsFst.Iterator(nil, nil)
	for err == nil {
		nul := 0
		key, _ := itr.Current()
		for i, b := range key {
			if b == 0x00 {
				nul = i
				break
			}
		}

		id := key[:nul]
		i := nul + 1

		season := binary.BigEndian.Uint32(key[i:])

		i += 4
		epnum := binary.BigEndian.Uint32(key[i:])

		i += 4
		tvShowID := key[i:]

		ep := &Episode{
			id:       string(id),
			tvShowID: string(tvShowID),
			season:   season,
			episode:  epnum,
		}

		fmt.Println(ep)
		err = itr.Next()
	}
	if err != nil {
		log.Fatal(err)
	}
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

		count++
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
