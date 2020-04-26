package main

import (
	"compress/gzip"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
)

func downloadAll() error {
	var wg sync.WaitGroup

	dataSets := []string{
		"title.akas.tsv.gz",
		"title.basics.tsv.gz",
		"title.episode.tsv.gz",
		"title.ratings.tsv.gz",
	}

	// make dir
	if err := os.Mkdir("data", 0700); err != nil {
		return err
	}

	if err := os.Mkdir("index", 0700); err != nil {
		return err
	}

	// loop over data sets and download one at a time
	// send to goroutine eventually
	for _, set := range dataSets {
		wg.Add(1)
		go download(set, &wg)
	}

	wg.Wait()

	return nil
}

// Downloads a single data set, decompresses it and writes it to the
// corresponding file path in the given directory.
func download(ds string, wg *sync.WaitGroup) error {
	defer wg.Done()
	// create out file
	f, err := os.OpenFile(path.Join("data", strings.TrimSuffix(ds, path.Ext(ds))), os.O_RDWR|os.O_CREATE, 0755)
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
	if err = writeSortedCSVRecords(r, f); err != nil {
		return err
	}

	return nil
}
