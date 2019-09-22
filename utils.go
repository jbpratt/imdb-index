package main

import "sort"

func sortTsv(data [][]string) {
	sort.Slice(data[:], func(i, j int) bool {
		for x := range data[i] {
			if data[i][x] == data[j][x] {
				continue
			}
			return data[i][x] < data[j][x]
		}
		return false
	})
}
