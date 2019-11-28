package main

import (
	"log"
	"os"
)

func main() {
	if _, err := os.Stat("./data"); os.IsNotExist(err) {
		err = downloadAll()
		if err != nil {
			log.Fatal(err)
		}
	}

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
