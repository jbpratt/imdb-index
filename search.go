package main

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
