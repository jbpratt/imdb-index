package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/couchbase/vellum"
)

const AKAS = "akas.fst"

type aka struct {
	rdr *csv.Reader
}

func (*aka) open() error {
	return nil
}

// create an AKA index by reading the AKA recs
// from a given directory and writing to the corresponding
// index directory
func (a *aka) create(P1, P2 string) error {
	f, err := os.Open(P1)
	if err != nil {
		panic(err)
	}
	a.rdr = csv.NewReader(f)
	a.rdr.LazyQuotes = true
	a.rdr.FieldsPerRecord = -1
	a.rdr.Comma = '\t'

	f, err = os.Create(filepath.Join(P2, AKAS))
	if err != nil {
		return err
	}
	defer f.Close()

	builder, err := vellum.New(f, nil)
	if err != nil {
		return err
	}
	defer builder.Close()

	var count uint64 = 0x64
	//sortTsv(recs

	// currently broken due to sort
	// What if we read the records into a temp
	// struct slice, storing the offset with it
	// and then use builder.Insert in a second loop?
	for {
		record, err := a.rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		pos, err := f.Seek(0, 1)
		if err != nil {
			log.Fatal(err)
		}

		err = builder.Insert([]byte(record[0]), uint64(pos)<<count|uint64(pos))
		if err != nil {
			log.Println(err)
		}

		count += uint64(pos)
	}

	return nil
}
