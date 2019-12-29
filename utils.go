package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/couchbase/vellum"
)

const IMDBBaseURL = "https://datasets.imdbws.com"

// The TSV file in the IMDb dataset that defines the canonical set of titles
// available to us. Each record contains basic information about a title,
// such as its IMDb identifier (e.g., `tt0096697`), primary title, start year
// and type. This includes movies, TV shows, episodes and more.
const IMDBBasics = "title.basics.tsv"

// The TSV file in the IMDb dataset that defines alternate names for some of
// the titles found in IMDB_BASICS. This includes, but is not limited to,
// titles in different languages. This file uses the IMDb identifier as a
// foreign key.
const IMDBAKAS = "title.akas.tsv"

// The TSV file in the IMDb dataset that defines the season and episode
// numbers for episodes in TV shows. Each record in this file corresponds to
// a single episode. There are four columns: the first is the IMDb identifier
// for the episode. The second is the IMDb identifier for the corresponding
// TV show. The last two columns are the season and episode numbers. Both of
// the IMDb identifiers are foreign keys that join the record to IMDB_BASICS.
const IMDBEpisode = "title.episode.tsv"

// The TSV file in the IMDb dataset that provides ratings for titles in
// IMDB_BASICS. Each title has at most one rating, and a rating corresponds
// to a rank (a decimal in the range 0-10) and the number of votes involved
// in creating that rating (from the IMDb web site, presumably).
const IMDBRatings = "title.ratings.ts"

var (
	ErrorUnknownTitle      = fmt.Errorf("unrecognized title type")
	ErrorUnknownScorer     = fmt.Errorf("unrecognized scorer name")
	ErrorUnknownNgramType  = fmt.Errorf("unrecognized ngram type")
	ErrorUnknownSimilarity = fmt.Errorf("unrecognized similarity function")
	ErrorUnknownDirective  = fmt.Errorf("unrecognized search directive")
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
// this will allow the caller to create a new csv reader
// after seeking to the desired position
// rename to openFile?
func openTsv(path string) (*csv.Reader, error) {
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

// Read all CSV data into memory and sort the records in lexicographic order.
//
// This is unfortunately necessary because the IMDb data is no longer sorted
// in lexicographic order with respect to the `tt` identifiers. This appears
// to be fallout as a result of adding 10 character identifiers (previously,
// only 9 character identifiers were used).
func writeSortedCSVRecords(in io.Reader, out io.Writer) error {
	// We actually only sort the raw lines here instead of parsing CSV records,
	// since parsing into CSV records has fairly substantial memory overhead.
	// Since IMDb CSV data never contains a record that spans multiple lines,
	// this transformation is okay.

	data := make([][]byte, 1000000)
	scanner := bufio.NewScanner(in)
	// remove duplicate rows
	for scanner.Scan() {
		data = append(data, scanner.Bytes())
		//prev := scanner.Text()
	}
	//sort.Slice(data, func(i int, j int) bool { return data[i] < data[j] })
	//_, err := out.Write(data)
	return nil
}

func fstMapFile(path string) (*vellum.Builder, error) {
	var buf bytes.Buffer
	return vellum.New(&buf, nil)
}
