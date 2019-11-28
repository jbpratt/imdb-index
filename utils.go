package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
)

const IMDBBaseURL = "https://datasets.imdbws.com"

/*func sortTsv(data [][]string) {
	sort.Slice(data[:], func(i, j int) bool {
		for x := range data[i] {
			if data[i][x] == data[j][x] {
				continue
			}
			return data[i][x] < data[j][x]
		}
		return false
	})
}*/

// TODO: just return the file and let caller handle
// this will allow the caller to create a new csv reader
// after seeking to the desired position
// rename to openFile?
func OpenTsv(path string) (*csv.Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	r.Comma = '\t'

	return r, nil
}

func openFile(path string) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return f, nil
}

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
		log.Fatal(err)
	}

	err = os.Mkdir("index", 0700)
	if err != nil {
		log.Fatal(err)
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

/// Downloads a single data set, decompresses it and writes it to the
/// corresponding file path in the given directory.
func download(ds string) error {

	// create out file
	f, err := os.OpenFile("data/"+filenameWithoutExtension(ds), os.O_RDWR|os.O_CREATE, 0755)
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

/// Read all CSV data into memory and sort the records in lexicographic order.
///
/// This is unfortunately necessary because the IMDb data is no longer sorted
/// in lexicographic order with respect to the `tt` identifiers. This appears
/// to be fallout as a result of adding 10 character identifiers (previously,
/// only 9 character identifiers were used).
func writeSortedCSVRecords(in io.Reader, out io.Writer) error {
	// We actually only sort the raw lines here instead of parsing CSV records,
	// since parsing into CSV records has fairly substantial memory overhead.
	// Since IMDb CSV data never contains a record that spans multiple lines,
	// this transformation is okay.

	data := make([][]byte, 1000000)
	scanner := bufio.NewScanner(in)
	// remove duplicate rows
	var prev string
	for scanner.Scan() {
		data = append(data, scanner.Bytes())
		prev = scanner.Text()
	}
	//sort.Slice(data, func(i int, j int) bool { return data[i] < data[j] })
	//_, err := out.Write(data)
	return nil
}

func filenameWithoutExtension(fn string) string {
	return strings.TrimSuffix(fn, path.Ext(fn))
}
