package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/couchbase/vellum"
	"github.com/jbpratt78/imdb-index/internal/types"
)

// EpisodeIndex allows for searching of tvshows and seasons indices
type EpisodeIndex struct {
	tvshows *vellum.FST
	seasons *vellum.FST
}

type EpisodeError string

func (e EpisodeError) Error() string {
	return string(e)
}

const (
	SEASONS = "episode.seasons.fst"
	TVSHOWS = "episode.tvshows.fst"
)

// EpisodeOpen opens an index from a previously created `Create` call
func EpisodeOpen(indexDir string) (*EpisodeIndex, error) {
	seasons, err := fstSetFile(path.Join(indexDir, SEASONS))
	if err != nil {
		return nil, err
	}

	tvshows, err := fstSetFile(path.Join(indexDir, TVSHOWS))
	if err != nil {
		return nil, err
	}

	return &EpisodeIndex{tvshows, seasons}, nil
}

// EpisodeCreate creates a new index and opens it
func EpisodeCreate(dataDir, indexDir string) (*EpisodeIndex, error) {
	fstShowFile := path.Join(indexDir, TVSHOWS)
	fstSeasonFile := path.Join(indexDir, SEASONS)
	tsv, err := os.Open(path.Join(dataDir, IMDBEpisode))
	if err != nil {
		return nil, err
	}
	defer tsv.Close()

	episodes, err := readSortedEpisodes(tsv)
	if err != nil {
		return nil, EpisodeError(fmt.Sprintf("failed to read episodes tsv: %v", err))
	}

	sort.Slice(episodes, func(i, j int) bool {
		if episodes[i].TvShowID != episodes[j].TvShowID {
			return episodes[i].TvShowID < episodes[j].TvShowID
		}
		if episodes[i].Season != episodes[j].Season {
			return episodes[i].Season < episodes[j].Season
		}
		if episodes[i].Episode != episodes[j].Episode {
			return episodes[i].Episode < episodes[j].Episode
		}
		return episodes[i].Id < episodes[j].Id
	})

	seasonBuilder, seasonIndexFile, err := fstSetBuilderFile(fstSeasonFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create fst set builder: %w", err)
	}

	for i, ep := range episodes {
		buffer, err := writeEpisode(ep)
		if err != nil {
			return nil, fmt.Errorf("failed to write episode: %w", err)
		}
		if err = seasonBuilder.Insert(buffer, uint64(i)); err != nil {
			return nil, fmt.Errorf("failed to insert episode into season builder: %w", err)
		}
	}

	if err = seasonBuilder.Close(); err != nil {
		return nil, fmt.Errorf("failed to close season builder: %w", err)
	}
	seasonIndexFile.Close()

	tvBuilder, tvIndexFile, err := fstSetBuilderFile(fstShowFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create fst set builder: %w", err)
	}

	sort.Slice(episodes, func(i, j int) bool {
		if episodes[i].Id != episodes[j].Id {
			return episodes[i].Id < episodes[j].Id
		}
		return episodes[i].TvShowID < episodes[j].TvShowID
	})

	for i, ep := range episodes {
		buffer, err := writeTvshow(ep)
		if err != nil {
			return nil, fmt.Errorf("failed to write tvshow: %w", err)
		}
		if err = tvBuilder.Insert(buffer, uint64(i)); err != nil {
			return nil, fmt.Errorf("failed to insert into tv builder: %w", err)
		}
	}

	if tvBuilder.Close(); err != nil {
		return nil, fmt.Errorf("failed to close tv builder: %w", err)
	}
	tvIndexFile.Close()

	return EpisodeOpen(indexDir)
}

func EpisodeRange(
	lower, upper []byte,
	fst *vellum.FST,
	readFunc func(key []byte) *types.Episode,
) ([]*types.Episode, error) {
	var eps []*types.Episode
	itr, err := fst.Iterator(lower, upper)
	if err != nil {
		return nil, err
	}

	for err == nil {
		key, _ := itr.Current()
		if key == nil {
			break
		}
		eps = append(eps, readFunc(key))
		err = itr.Next()
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		return eps, nil
	}
	return nil, EpisodeError("iterator did not finish")
}

func (i *EpisodeIndex) Seasons(tvshowId []uint8, season uint32) ([]*types.Episode, error) {
	return EpisodeRange(tvshowId, append(tvshowId, 0xFF), i.seasons, readEpisode)
}

func (i *EpisodeIndex) Episodes(tvshowId []uint8, season uint32) ([]*types.Episode, error) {
	lower := append(tvshowId, 0x00)
	upper := append(tvshowId, 0x00)
	buff := make([]byte, 4)

	binary.BigEndian.PutUint32(buff, season)
	lower = append(lower, buff...)

	buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, 0)
	lower = append(lower, buff...)

	buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, season)
	upper = append(upper, buff...)

	buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, ^uint32(0))
	upper = append(upper, buff...)

	return EpisodeRange(lower, upper, i.seasons, readEpisode)
}

func (i *EpisodeIndex) Episode(epId []uint8) (*types.Episode, error) {
	eps, err := EpisodeRange(epId, append(epId, 0xFF), i.tvshows, readTvshow)
	if err != nil {
		return nil, err
	}
	return eps[0], nil
}

func readSortedEpisodes(in *os.File) ([]*types.Episode, error) {
	var episodes []*types.Episode
	header := []string{}

	csvReader := csvRBuilder(in)

	// read sorted episodes
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if len(header) == 0 {
			header = rec
			continue
		}

		season, err := strconv.ParseUint(rec[2], 10, 32)
		if err != nil {
			fmt.Println("failed to parse", rec)
			continue
		}
		episode, err := strconv.ParseUint(rec[3], 10, 32)
		if err != nil {
			continue
		}

		episodes = append(episodes, &types.Episode{
			Id:       rec[0],
			TvShowID: rec[1],
			Season:   uint32(season),
			Episode:  uint32(episode),
		})
	}
	return episodes, nil
}

func readEpisode(key []byte) *types.Episode {
	nul := 0
	for i, b := range key {
		if b == 0x00 {
			nul = i
			break
		}
	}

	tvShowID := key[:nul]
	i := nul + 1

	season := binary.BigEndian.Uint32(key[i:])

	i += 4
	epnum := binary.BigEndian.Uint32(key[i:])

	i += 4
	id := key[i:]

	return &types.Episode{
		Id:       string(id),
		TvShowID: string(tvShowID),
		Season:   season,
		Episode:  epnum,
	}
}

func writeEpisode(ep *types.Episode) ([]uint8, error) {
	buffer := []uint8{}
	for _, b := range []byte(ep.TvShowID) {
		if b == 0 {
			return nil, EpisodeError(fmt.Sprintf("unsupported rating id with nil byte for %v", ep))
		}
	}

	buffer = append(buffer, []uint8(ep.TvShowID)...)
	buffer = append(buffer, 0x00)

	y := make([]byte, 4)
	binary.BigEndian.PutUint32(y, valOrMax(ep.Season))
	buffer = append(buffer, y...)

	y = make([]byte, 4)
	binary.BigEndian.PutUint32(y, valOrMax(ep.Episode))
	buffer = append(buffer, y...)
	buffer = append(buffer, []uint8(ep.Id)...)

	return buffer, nil
}

func readTvshow(key []byte) *types.Episode {
	nul := 0
	for i, b := range key {
		if b == 0x00 {
			nul = i
			break
		}
	}

	id := key[:nul]
	i := nul + 1

	season := binary.BigEndian.Uint32(key[i:])

	i += 4
	epnum := binary.BigEndian.Uint32(key[i:])

	i += 4
	tvShowID := key[i:]

	return &types.Episode{
		Id:       string(id),
		TvShowID: string(tvShowID),
		Season:   season,
		Episode:  epnum,
	}
}

func writeTvshow(ep *types.Episode) ([]uint8, error) {
	buffer := []uint8{}
	for _, b := range []byte(ep.Id) {
		if b == 0 {
			return nil, EpisodeError(fmt.Sprintf("unsupported rating id with nil byte for %v", ep))
		}
	}

	buffer = append(buffer, []uint8(ep.Id)...)
	buffer = append(buffer, 0x00)

	y := make([]byte, 4)
	binary.BigEndian.PutUint32(y, valOrMax(ep.Season))
	buffer = append(buffer, y...)

	y = make([]byte, 4)
	binary.BigEndian.PutUint32(y, valOrMax(ep.Episode))
	buffer = append(buffer, y...)

	buffer = append(buffer, []uint8(ep.TvShowID)...)

	return buffer, nil
}

func valOrMax(val uint32) uint32 {
	if val != 0 {
		return val
	} else if val == ^uint32(0) {
		log.Fatal(EpisodeError("unsupported number"))
	}
	return ^uint32(0)
}
