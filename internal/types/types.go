package types

// TitleKind is the kind of titles available
type TitleKind string

const (
	Movie        TitleKind = "movie"
	Short                  = "short"
	TVEpisode              = "tvEpisode"
	TVMiniSeries           = "tvMiniSeries"
	TVMovie                = "tvMovie"
	TVSeries               = "tvSeries"
	TVShort                = "tvShort"
	TVSpecial              = "tvSpecial"
	Video                  = "video"
	VideoGame              = "videoGame"
)

// Query is for searching records
type Query struct {
	name        string
	name_scorer interface{}
	similarity  interface{}
	size        uint
	kinds       []TitleKind
	year        uint32
	votes       uint32
	season      uint32
	episode     uint32
	tvShowID    string
}

// Title is An IMDb title record.
//
// This is the primary type of an IMDb media entry. This record defines the
// identifier of an IMDb title, which serves as a foreign key in other data
// files (such as alternate names, episodes and ratings).
type Title struct {
	// An IMDb identifier.
	//
	// Generally, this is a fixed width string beginning with the characters
	// `tt`.
	Id string
	// The specific type of a title, e.g., movie, TV show, episode, etc.
	Kind TitleKind
	// The primary name of this title.
	Title string
	// The "original" name of this title.
	OriginalTitle string
	// Whether this title is classified as "adult" material or not.
	IsAdult bool
	// The start year of this title.
	//
	// Generally, things like movies or TV episodes have a start year to
	// indicate their release year and no end year. TV shows also have a start
	// year. TV shows that are still airing lack an end time, but TV shows
	// that have stopped will typically have an end year indicating when it
	// stopped airing.
	//
	// Note that not all titles have a start year.
	StartYear uint32
	// The end year of this title.
	//
	// This is typically used to indicate the ending year of a TV show that
	// has stopped production.
	EndYear uint32
	// The runtime, in minutes, of this title.
	RuntimeMinutes uint32
	// A comma separated string of genres.
	Genres string
}

// Aka is a single alternate name.
//
// Every title has one or more names, and zero or more alternate names. To
// represent multiple names, AKA or "also known as" records are provided.
// There may be many AKA records for a single title.
type Aka struct {
	// The IMDb identifier that these AKA records describe.
	Id string
	// The order in which an AKA record should be preferred.
	Order int32
	// The alternate name.
	Title string
	// A geographic region in which this alternate name applies.
	Region string
	// The language of this alternate name.
	Language string
	// A comma separated list of types for this name.
	Types string
	// A comma separated list of attributes for this name.
	Attributes      string
	IsOriginalTitle bool
}

// Episode is a single episode record.
//
// An episode record is an entry that joins two title records together, and
// provides episode specific information, such as the season and episode
// number. The two title records joined correspond to the title record for the
// TV show and the title record for the episode.
type Episode struct {
	// The IMDb title identifier for this episode.
	Id string
	// The IMDb title identifier for the parent TV show of this episode.
	TvShowID string
	// The season in which this episode is contained, if it exists.
	Season uint32
	// The episode number of the season in which this episode is contained, if
	// it exists.
	Episode uint32
}

// A rating associated with a single title record.
type Rating struct {
	// The IMDb title identifier for this rating.
	Id string
	// The rating, on a scale of 0 to 10, for this title.
	Rating float32
	// The number of votes involved in this rating.
	Votes  uint32
	Offset uint64
}
