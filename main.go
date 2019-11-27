package main

func main() {
	// check if path exists
	err := createAka()
	if err != nil {
		panic(err)
	}

	_, err = openAka()
	if err != nil {
		panic(err)
	}

	//aka.find()

}
