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
)

const AKAS = "akas.fst"

type AkasIndex struct {
	idx *vellum.FST
	sr  *io.SectionReader
}

func AkasOpen(indexDir, dataDir string) (*AkasIndex, error) {
	idx, err := fstSetFile(path.Join(indexDir, AKAS))
	if err != nil {
		return nil, err
	}
	sr, err := mmapReader(path.Join(dataDir, IMDBAKAS))
	if err != nil {
		return nil, err
	}

	return &AkasIndex{idx, sr}, nil
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
			return nil, fmt.Errorf("failed to insert aka: %v %w", aka, err)
		}
	}

	akasIndexFile.Close()

	return AkasOpen(indexDir, dataDir)
}

func (a *AkasIndex) Find(id []uint8) ([]*types.Aka, error) {
	v, valid, err := a.idx.Get(id)
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, fmt.Errorf("failed to find %q", id)
	}

	count := v >> 48
	offset := v & ((1 << 48) - 1)

	if _, err := a.sr.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	csvr := csvRBuilder(a.sr)
	for i := 0; i < int(count); i++ {
		_, err := csvr.Read()
		if err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("unimplemented")
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
	return akas, nil
}
