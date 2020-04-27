package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strconv"

	"github.com/couchbase/vellum"
	"github.com/jbpratt78/imdb-index/internal/types"
)

const RATINGS = "ratings.fst"

type RatingsError string

func (e RatingsError) Error() string { return string(e) }

type RatingsIndex struct {
	idx *vellum.FST
}

func RatingsOpen(indexDir string) (*RatingsIndex, error) {
	idx, err := FstSetFile(path.Join(indexDir, RATINGS))
	if err != nil {
		return nil, err
	}
	return &RatingsIndex{idx}, nil
}

func RatingsCreate(dataDir, indexDir string) (*RatingsIndex, error) {

	fstRatingsFile := path.Join(indexDir, "ratings.fst")
	tsv, err := os.Open(path.Join(dataDir, IMDBRatings))
	if err != nil {
		return nil, err
	}
	defer tsv.Close()

	var count uint64 = 0
	var buf bytes.Buffer
	var offset int64
	var buffer []uint8
	header := []string{}

	ratingsBuilder, ratingsIndexFile, err := FstSetBuilderFile(fstRatingsFile)
	if err != nil {
		return nil, err
	}

	csvReader := CsvRBuilder(tsv)
	// loop
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
				return nil, RatingsError(fmt.Sprintf("unsupported rating id with nil byte for %v", rating))
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

		x = make([]byte, 4)
		binary.BigEndian.PutUint32(x, record.Votes)
		for _, u := range x {
			buffer = append(buffer, u)
		}

		if err = ratingsBuilder.Insert(buffer, uint64(offset)); err != nil {
			panic(err)
		}

		count++
	}

	if err = ratingsBuilder.Close(); err != nil {
		return nil, fmt.Errorf("failed to create fst set builder: %w", err)
	}
	ratingsIndexFile.Close()

	return RatingsOpen(indexDir)
}
