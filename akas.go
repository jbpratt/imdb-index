package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/couchbase/vellum"
	"github.com/pkg/errors"
)

const AKAS = "Akas.fst"

type Akas struct {
	idx *csv.Reader
	fst *vellum.FST
}

type AkaRecord struct {
	pos uint64
	rec string
}

func AkaOpen(P1, P2 string) (*Akas, error) {
	r, err := OpenTsv(P1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open tsv")
	}
	fst, err := vellum.Open(filepath.Join(P2, AKAS))
	if err != nil {
		return nil, errors.Wrap(err, "failed to open fst")
	}

	return &Akas{idx: r, fst: fst}, nil
}

// create an Akas index by reading the Akas recs
// from a given directory and writing to the corresponding
// index directory
func AkaCreate(P1, P2 string) (*Akas, error) {
	r, err := OpenTsv(P1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open tsv")
	}
	f, err := os.Create(filepath.Join(P2, AKAS))
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

	outRecords := []AkaRecord{}

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err)
		}

		pos, err := f.Seek(0, 1)
		if err != nil {
			log.Println(err)
		}

		outRecords = append(outRecords, AkaRecord{uint64(pos), rec[0]})
	}

	sort.Slice(outRecords, func(i, j int) bool {
		return outRecords[i].rec < outRecords[j].rec
	})

	for _, x := range outRecords {
		err = builder.Insert([]byte(x.rec), x.pos<<count|x.pos)
		if err != nil {
			log.Println(err)
		}

		count += uint64(x.pos)
	}

	return AkaOpen(P1, P2)
}

/*
func (a *Akas) find(id string) error {

	v, ex, err := a.fst.Get([]byte(id))
	if err != nil {
		return errors.Wrap(err, "failed to get value")
	}
	if ex {
		count := v >> 48
		offset := v & ((1 << 48) - 1)
		_ = count
		_ = offset
	}
	return errors.New("not implemented")
}*/
