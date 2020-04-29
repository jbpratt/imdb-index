package main

import "testing"

// index gets setup in episode_test.go:TestMain
func TestRatingBasic(t *testing.T) {
	idx, err := RatingsOpen(tmpDir)
	if err != nil {
		t.Fatalf("failed to open ratings index: %v", err)
	}

	rating, err := idx.Rating([]byte("tt0000001"))
	if err != nil {
		t.Fatalf("failed to get rating: %v", err)
	}

	if rating.Rating != 5.8 {
		t.Fatalf("incorrect rating: %f", rating.Rating)
	}

	if rating.Votes != 1356 {
		t.Fatalf("incorrect votes: %d", rating.Votes)
	}
}
