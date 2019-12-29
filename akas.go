package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/couchbase/vellum"
	"github.com/pkg/errors"
)

const AKAS = "akas.fst"

type AkaIndex struct {
	akas *csv.Reader
	idx  *IndexReader
}

type AkaReader struct {
	idx *csv.Reader
	fst *vellum.FST
}

type AkaRecord struct {
	id  []uint8
	pos uint64
	rec string
}

func openAka() (*AkaReader, error) {
	idx, err := openTsv("data/" + IMDBAKAS)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open tsv")
	}
	fst, err := vellum.Open(AKAS)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open fst")
	}

	return &AkaReader{idx: idx, fst: fst}, nil
}

// create an Akas index by reading the Akas recs
// from a given directory and writing to the corresponding
// index directory
func createAka() error {
	r, err := openTsv("data/" + IMDBAKAS)
	if err != nil {
		return fmt.Errorf("failed to open tsv: %v", err)
	}
	f, err := os.Create("index/" + AKAS)
	if err != nil {
		return fmt.Errorf("failed to create index file: %v", err)
	}
	defer f.Close()

	builder, err := vellum.New(f, nil)
	if err != nil {
		return fmt.Errorf("failed to create index builder: %v", err)
	}
	defer builder.Close()

	var count uint64 = 0x64

	outRecords := []AkaRecord{}

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err)
			return err
		}

		pos, err := f.Seek(0, 1)
		if err != nil {
			log.Println(err)
			return err
		}

		outRecords = append(outRecords, AkaRecord{pos: uint64(pos), rec: rec[0]})
	}

	sort.Slice(outRecords, func(i, j int) bool {
		return outRecords[i].rec < outRecords[j].rec
	})

	for _, x := range outRecords {
		err = builder.Insert([]byte(x.rec), x.pos<<count|x.pos)
		if err != nil {
			log.Println(err)
			return err
		}

		count += uint64(x.pos)
	}

	return nil
}

func (a *AkaReader) find(id string) error {

	v, ex, err := a.fst.Get([]byte(id))
	if err != nil {
		return errors.Wrap(err, "failed to get value")
	}
	if ex {
		count := v >> 48
		offset := v & ((1 << 48) - 1)
		_ = count
		_ = offset
		// need to seek to place in csv where located
		// I think I just want a bytes.Reader
	}
	return errors.New("not implemented")
}
