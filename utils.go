package main

import (
	"encoding/csv"
	"os"
)

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
// this will allow the caller to create a new reader
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

func download_all() error {
	dataSets := []string{
		"title.akas.tsv.gz",
		"title.basics.tsv.gz",
		"title.episode.tsv.gz",
		"title.ratings.tsv.gz",
	}

	// make dir

	// loop over data sets and download one at a time
	for _, set := range dataSets {
		_ = set
	}

	return nil
}

/// Downloads a single data set, decompresses it and writes it to the
/// corresponding file path in the given directory.
func download() error {

	// create file

	// format url

	// make request

	// decode Gz file

	// sort and write

	return nil
}

/// Read all CSV data into memory and sort the records in lexicographic order.
///
/// This is unfortunately necessary because the IMDb data is no longer sorted
/// in lexicographic order with respect to the `tt` identifiers. This appears
/// to be fallout as a result of adding 10 character identifiers (previously,
/// only 9 character identifiers were used).
func writeSortedCSVRecords() error {
	return nil
}
