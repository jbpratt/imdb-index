package main

func main() {
	a := aka{}
	a.create("data/title.akas.tsv", "index/")
}

/*func createIndex() error {
	err := filepath.Walk("./data", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
		}
		if info.IsDir() && info.Name() == "data" {
			log.Println("skipping dir")
		}
		return createFST(strings.TrimSuffix(path, ".tsv")+".fst", readTsv(path))
	})
	if err != nil {
		return err
	}
	return nil
}

func readIndex() error {
	return nil
}

func readTsv(path string) [][]string {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	r.Comma = '\t'

	recs, err := r.ReadAll()
	if err != nil {
		return nil
	}
	return recs
}*/

/*func createFST(name string, data [][]string) error {
	if data == nil {
		return nil
	}
	if _, err := os.Stat("index"); os.IsNotExist(err) {
		os.Mkdir("index", os.ModePerm)
	}
	name = strings.TrimPrefix(name, "data/")
	f, err := os.Create(filepath.Join("index", name))
	if err != nil {
		return err
	}
	log.Println("created", filepath.Join("index", name))
	builder, err := vellum.New(f, nil)
	if err != nil {
		return err
	}
	defer builder.Close()

	sortTsv(data)

	for i, d := range data {
		err = builder.Insert([]byte(d[0]), uint64(i))
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}*/

func loadFST(path string) ([][]string, error) {
	return nil, nil
}

// handled with above flags for csv.Reader
/*if perr, ok := err.(*csv.ParseError); ok && (perr.Err == csv.ErrFieldCount) {
	fmt.Println(perr, rec)
	continue
}*/
