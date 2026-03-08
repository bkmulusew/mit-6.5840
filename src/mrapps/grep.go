package main

//
// a grep application "plugin" for MapReduce.
//
// go build -buildmode=plugin grep.go
//
// Set GREP_PATTERN env var to the search string before running workers.
//

import (
	"fmt"
	"os"
	"strings"

	"6.5840/mr"
)

func Map(filename string, contents string) []mr.KeyValue {
	pattern := os.Getenv("GREP_PATTERN")
	if pattern == "" {
		return nil
	}

	var kva []mr.KeyValue
	for i, line := range strings.Split(contents, "\n") {
		if strings.Contains(line, pattern) {
			key := fmt.Sprintf("%s:%d", filename, i+1)
			kva = append(kva, mr.KeyValue{Key: key, Value: line})
		}
	}
	return kva
}

func Reduce(key string, values []string) string {
	return values[0]
}
