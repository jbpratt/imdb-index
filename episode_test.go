package main

import (
	"errors"
	"os"
	"testing"

	"github.com/couchbase/vellum"
)

var tmpDir string

func TestMain(m *testing.M) {
	tmpDir = os.TempDir()
	defer os.RemoveAll(tmpDir)

	_, err := EpisodeCreate("testdata", tmpDir)
	if err != nil {
		panic(err)
	}

	_, err = RatingsCreate("testdata", tmpDir)
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func TestEpisodeBasic(t *testing.T) {
	idx, err := EpisodeOpen(tmpDir)
	if err != nil {
		t.Fatalf("failed to open episode indicies: %v", err)
	}
	eps, err := idx.Seasons([]byte("tt0096697"), 2)
	if err != nil {
		if errors.Is(err, vellum.ErrIteratorDone) {
		} else {
			panic(err)
		}
	}

	counts := make(map[uint32]uint32)
	for _, ep := range eps {
		counts[ep.Season] += 1
	}

	if len(counts) != 3 {
		t.Fatalf("got the wrong amount of episodes: got=%d want=%d", len(counts), 3)
	}
	if counts[1] != 13 {
		t.Fatalf("got the wrong count: got=%d want=%d", counts[1], 13)
	}
	if counts[2] != 22 {
		t.Fatalf("got the wrong count: got=%d want=%d", counts[2], 22)
	}
	if counts[3] != 24 {
		t.Fatalf("got the wrong count: got=%d want=%d", counts[3], 24)
	}
}

func TestBySeason(t *testing.T) {
	idx, err := EpisodeOpen(tmpDir)
	if err != nil {
		t.Fatalf("failed to create indices: %v", err)
	}
	eps, err := idx.Episodes([]byte("tt0096697"), 2)
	if err != nil {
		if errors.Is(err, vellum.ErrIteratorDone) {
		} else {
			panic(err)
		}
	}
	counts := make(map[uint32]uint32)
	for _, ep := range eps {
		counts[ep.Season] += 1
	}

	if len(counts) != 1 {
		t.Fatalf("got the wrong amount of counts: got=%d want=%d", len(counts), 1)
	}
	if counts[2] != 22 {
		t.Fatalf("got the wrong count: got=%d want=%d", counts[2], 22)
	}
}

func TestTvshow(t *testing.T) {
	idx, err := EpisodeOpen(tmpDir)
	if err != nil {
		t.Fatalf("failed to create indices: %v", err)
	}
	want := "tt0096697"

	ep, err := idx.Episode([]byte("tt0701063"))
	if err != nil {
		if errors.Is(err, vellum.ErrIteratorDone) {
		} else {
			panic(err)
		}
	}

	if ep.TvShowID != want {
		t.Fatalf("incorrect tvshowid: got=%q want=%q", ep.TvShowID, want)
	}
}
