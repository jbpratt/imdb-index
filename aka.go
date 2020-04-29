package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sort"

	"github.com/couchbase/vellum"
	"github.com/jbpratt78/imdb-index/internal/types"
	"golang.org/x/exp/mmap"
)

const AKAS = "akas.fst"

type AkasIndex struct {
	idx *vellum.FST
	csv *mmap.ReaderAt
}

func AkasOpen(indexDir string) (*AkasIndex, error) {
	idx, err := fstSetFile(path.Join(indexDir, AKAS))
	if err != nil {
		return nil, err
	}
	return &AkasIndex{idx, nil}, nil
}

func AkasCreate(dataDir, indexDir string) (*AkasIndex, error) {
	fstAkasFile := path.Join(indexDir, AKAS)
	tsv, err := os.Open(path.Join(dataDir, IMDBAKAS))
	if err != nil {
		return nil, err
	}
	defer tsv.Close()

	akasBuilder, akasIndexFile, err := fstSetBuilderFile(fstAkasFile)
	if err != nil {
		return nil, err
	}

	akas, err := readSortedAkas(tsv)
	if err != nil {
		return nil, fmt.Errorf("failed to read akas tsv: %w", err)
	}

	for _, aka := range akas {
		if err = akasBuilder.Insert([]byte(aka.Id), (aka.Count<<48)|aka.Offset); err != nil {
			panic(err)
		}
	}

	akasIndexFile.Close()

	return AkasOpen(indexDir)
}

func readSortedAkas(in io.Reader) ([]*types.Aka, error) {
	var count uint64 = 0
	var buf bytes.Buffer
	var offset uint64

	header := []string{}
	akas := []*types.Aka{}
	tr := io.TeeReader(in, &buf)
	csvReader := csvRBuilder(tr)

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

		aka := &types.Aka{Id: rec[0], Offset: offset, Count: count}
		akas = append(akas, aka)
		count += aka.Count
	}

	sort.Slice(akas, func(i, j int) bool {
		return akas[i].Id < akas[j].Id
	})
	return nil, fmt.Errorf("unimplemented")
}
