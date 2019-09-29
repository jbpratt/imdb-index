package main

import (
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"

	"github.com/couchbase/vellum"
)

const RATINGS = "ratings.fst"

type Ratings struct {
	idx *csv.Reader
	fst *vellum.FST
}

type RatingRecord struct {
	pos uint64
	rec string
}

/// Create a rating index from the given IMDb data directory, and write it
/// to the given index directory. If a rating index already exists, then it
/// is overwritten.
func RatingCreate(P1, P2 string) (*Ratings, error) {
	r, err := OpenTsv(P1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open tsv")
	}
	f, err := os.Create(filepath.Join(P2, RATINGS))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create index file")
	}
	defer f.Close()

	builder, err := vellum.New(f, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create index builder")
	}
	defer builder.Close()

	var count uint64 = 0x64

	outRecords := []RatingRecord{}

	return nil, errors.New("not implemented")
}
