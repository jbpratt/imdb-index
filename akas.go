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

const AKAS = "akas.fst"

type akas struct {
	idx *csv.Reader
	fst *vellum.FST
}

type akaRecord struct {
	pos uint64
	rec string
}

func akaOpen(P1, P2 string) (*akas, error) {
	f, err := os.Open(P1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open tsv")
	}
	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	r.Comma = '\t'

	fst, err := vellum.Open(filepath.Join(P2, AKAS))
	if err != nil {
		return nil, errors.Wrap(err, "failed to open fst")
	}

	return &akas{idx: r, fst: fst}, nil
}

// create an akas index by reading the akas recs
// from a given directory and writing to the corresponding
// index directory
func akaCreate(P1, P2 string) (*akas, error) {
	f, err := os.Open(P1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open tsv")
	}
	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	r.Comma = '\t'

	f, err = os.Create(filepath.Join(P2, AKAS))
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
	//sortTsv(recs

	outRecords := []akaRecord{}
	// currently broken due to sort
	// What if we read the records into a temp
	// struct slice, storing the offset with it
	// sort this slice
	// and then use builder.Insert in a second loop?
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

		outRecords = append(outRecords, akaRecord{uint64(pos), rec[0]})
	}

	sortSlice(outRecords)

	for _, x := range outRecords {
		err = builder.Insert([]byte(x.rec), x.pos<<count|x.pos)
		if err != nil {
			log.Println(err)
		}

		count += uint64(x.pos)
	}

	return akaOpen(P1, P2)
}

func (a *akas) find(id string) error {

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
}

func sortSlice(data []akaRecord) {
	sort.Slice(data, func(i, j int) bool {
		return data[i].rec < data[j].rec
	})
}
