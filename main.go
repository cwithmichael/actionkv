package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
)

var Usage string

func init() {
	if runtime.GOOS == "windows" {
		Usage = `Usage:
		akv_mem.exe FILE get KEY
		akv_mem.exe FILE delete KEY
		akv_mem.exe FILE insert KEY VALUE
		akv_mem.exe FILE update KEY VALUE
		`
	} else {
		Usage = `Usage:
		akv_mem FILE get KEY
		akv_mem FILE delete KEY
		akv_mem FILE insert KEY VALUE
		akv_mem FILE update KEY VALUE
		`
	}
}

func main() {
	flag.Parse()
	if flag.NArg() < 3 {
		fmt.Println(Usage)
		os.Exit(1)
	}
	values := flag.Args()
	fname := values[0]
	action := values[1]
	key := values[2]
	var maybeValue string
	if len(values) == 4 {
		maybeValue = values[3]
	}

	store, err := NewActionKV(fname)
	if err != nil {
		log.Fatalf("Unable to open file: %s", fname)
	}
	defer store.BackingFile.Close()

	err = store.Load()
	if err != nil {
		log.Fatalf("Unable to load data: %s\n", err.Error())
	}

	switch action {
	case "get":
		value, err := store.Get(ByteString(key))
		if err != nil {
			log.Fatalf("%s not found: %s\n", key, err.Error())
		}
		fmt.Printf("Value: %s\n", value)
	case "delete":
		err := store.Delete(ByteString(key))
		if err != nil {
			log.Fatalf("Failed to delete %s\n", key)
		}
	case "insert":
		if len(values) < 4 {
			fmt.Println(Usage)
			os.Exit(1)
		}
		err := store.Insert(ByteString(key), ByteString(maybeValue))
		if err != nil {
			log.Fatalf("Failed to insert %s, %s\n", key, maybeValue)
		}
	case "update":
		if len(values) < 4 {
			fmt.Println(Usage)
			os.Exit(1)
		}
		err := store.Update(ByteString(key), ByteString(maybeValue))
		if err != nil {
			log.Fatalf("Failed to update %s, %s\n", key, maybeValue)
		}
	default:
		fmt.Println(Usage)
	}
}
