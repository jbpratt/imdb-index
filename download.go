package main

import (
	"compress/gzip"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
)

func downloadAll() error {
	dataSets := []string{
		"title.akas.tsv.gz",
		"title.basics.tsv.gz",
		"title.episode.tsv.gz",
		"title.ratings.tsv.gz",
	}

	// make dir
	err := os.Mkdir("data", 0700)
	if err != nil {
		return err
	}

	err = os.Mkdir("index", 0700)
	if err != nil {
		return err
	}

	// loop over data sets and download one at a time
	// send to goroutine eventually
	for _, set := range dataSets {
		err = download(set)
		if err != nil {
			panic(err)
		}
	}

	return nil
}

// Downloads a single data set, decompresses it and writes it to the
// corresponding file path in the given directory.
func download(ds string) error {

	// create out file
	f, err := os.OpenFile("data/"+strings.TrimSuffix(ds, path.Ext(ds)), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	resp, err := http.Get(fmt.Sprintf("%s/%s", IMDBBaseURL, ds))
	if err != nil {
		return err
	}

	// decode Gz file
	r, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer r.Close()

	// sort and write
	err = writeSortedCSVRecords(r, f)
	if err != nil {
		return err
	}

	return nil
}
