package main

import "github.com/couchbase/vellum"

type IndexReader struct {
	idx *vellum.FST
}

func fromPath(path string) (*IndexReader, error) {
	return nil, nil
}
