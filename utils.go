package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/couchbase/vellum"
	"golang.org/x/sync/errgroup"
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
const IMDBRatings = "title.ratings.tsv"

var (
	ErrorUnknownTitle      = fmt.Errorf("unrecognized title type")
	ErrorUnknownScorer     = fmt.Errorf("unrecognized scorer name")
	ErrorUnknownNgramType  = fmt.Errorf("unrecognized ngram type")
	ErrorUnknownSimilarity = fmt.Errorf("unrecognized similarity function")
	ErrorUnknownDirective  = fmt.Errorf("unrecognized search directive")
)

func DownloadAll(dir string) error {
	dataSets := []string{
		"title.akas.tsv.gz",
		"title.basics.tsv.gz",
		"title.episode.tsv.gz",
		"title.ratings.tsv.gz",
	}

	// make dir
	if err := os.Mkdir(path.Join(dir, "data"), os.ModePerm); err != nil {
		return err
	}

	errs, _ := errgroup.WithContext(context.Background())
	for _, set := range dataSets {
		set := set
		errs.Go(func() error {
			return download(set, dir)
		})
	}

	if err := errs.Wait(); err != nil {
		return err
	}
	return nil
}

// Downloads a single data set, decompresses it and writes it to the
// corresponding file path in the given directory.
func download(file, outdir string) error {
	outfile := path.Join(outdir, "data", strings.TrimSuffix(file, path.Ext(file)))
	f, err := os.OpenFile(outfile, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	resp, err := http.Get(fmt.Sprintf("%s/%s", IMDBBaseURL, file))
	if err != nil {
		return err
	}

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
	var data []string
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		data = append(data, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	sort.Strings(data[1:])

	w := bufio.NewWriter(out)
	prev := ""
	for i, d := range data {
		first := strings.Split(d, "\t")[0]
		if i > 0 && first == prev {
			continue
		}

		prev = first
		fmt.Fprintln(w, d)
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return nil
}

// FstSetFile opens an FST set file for the given path as a memory map
func FstSetFile(path string) (*vellum.FST, error) {
	set, err := vellum.Open(path)
	if err != nil {
		return nil, err
	}
	return set, nil
}

// FstSetFile opens an FST set file for the given path as a memory map
func FstSetBuilderFile(path string) (*vellum.Builder, *os.File, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}

	set, err := vellum.New(file, nil)
	if err != nil {
		return nil, nil, err
	}
	return set, file, nil
}

func CsvRBuilder(in io.Reader) *csv.Reader {
	csvReader := csv.NewReader(in)
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	return csvReader
}
