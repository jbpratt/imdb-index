package main

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	idx, err := fstSetFile(path.Join(indexDir, RATINGS))
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

	ratingsBuilder, ratingsIndexFile, err := fstSetBuilderFile(fstRatingsFile)
	if err != nil {
		return nil, err
	}

	ratings, err := readSortedRatings(tsv)
	if err != nil {
		return nil, RatingsError(fmt.Sprintf("failed to read ratings tsv: %v", err))
	}

	for _, r := range ratings {
		buffer, err := writeRating(r)
		if err != nil {
			return nil, fmt.Errorf("failed to write rating: %w", err)
		}

		if err = ratingsBuilder.Insert(buffer, r.Offset); err != nil {
			return nil, fmt.Errorf("failed to insert rating into ratings builder: %w", err)
		}
	}

	if err = ratingsBuilder.Close(); err != nil {
		return nil, fmt.Errorf("failed to create fst set builder: %w", err)
	}
	ratingsIndexFile.Close()

	return RatingsOpen(indexDir)
}

func ratingsRange(
	lower, upper []byte,
	fst *vellum.FST,
	readFunc func(key []byte) *types.Rating,
) ([]*types.Rating, error) {
	var ratings []*types.Rating
	itr, err := fst.Iterator(lower, upper)
	if err != nil {
		return nil, err
	}

	for err == nil {
		key, _ := itr.Current()
		if key == nil {
			break
		}
		ratings = append(ratings, readFunc(key))
		err = itr.Next()
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		return ratings, nil
	}
	return nil, RatingsError("iterator did not finish")
}

func (i *RatingsIndex) Rating(id []uint8) (*types.Rating, error) {
	upper := append(id, 0xFF)
	ratings, err := ratingsRange(id, upper, i.idx, readRating)
	if err != nil {
		return nil, err
	}
	return ratings[0], nil
}

func readSortedRatings(in *os.File) ([]*types.Rating, error) {
	ratings := []*types.Rating{}
	var count uint64 = 0
	var buf bytes.Buffer
	var offset uint64
	header := []string{}
	tr := io.TeeReader(in, &buf)
	csvReader := csvRBuilder(tr)
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read csv: %w", err)
		}

		if len(header) == 0 {
			header = rec
			continue
		}

		// get offset
		line, err := buf.ReadBytes('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to get offset for %v got %w", rec, err)
		}

		length := len(line)
		offset += uint64(length)

		rating, err := strconv.ParseFloat(rec[1], 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse rating for %v got %w", rec, err)
		}

		votes, err := strconv.ParseUint(rec[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse votes for %v got %w", rec, err)
		}

		ratings = append(ratings, &types.Rating{Offset: offset, Id: rec[0], Rating: float32(rating), Votes: uint32(votes)})

		count++
	}
	return ratings, nil
}

func writeRating(rt *types.Rating) ([]uint8, error) {
	buffer := []uint8{}
	for _, b := range []byte(rt.Id) {
		if b == 0 {
			return nil, RatingsError(fmt.Sprintf("unsupported rating id with nil byte for %v", rt))
		}
	}
	// append id
	buffer = append(buffer, []uint8(rt.Id)...)
	buffer = append(buffer, 0x00)

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, math.Float32bits(rt.Rating))
	buffer = append(buffer, x...)

	x = make([]byte, 4)
	binary.BigEndian.PutUint32(x, rt.Votes)
	buffer = append(buffer, x...)
	return buffer, nil
}

func readRating(key []byte) *types.Rating {
	nul := 0
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
	return &types.Rating{Id: string(id), Rating: rating, Votes: votes}
}
