package main

import (
	"bytes"
	"encoding/gob"
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
		actionkv.exe FILE get KEY
		actionkv.exe FILE delete KEY
		actionkv.exe FILE insert KEY VALUE
		actionkv.exe FILE update KEY VALUE
		`
	} else {
		Usage = `Usage:
		actionkv FILE get KEY
		actionkv FILE delete KEY
		actionkv FILE insert KEY VALUE
		actionkv FILE update KEY VALUE
		`
	}
}

func storeIndexOnDisk(akv *ActionKV, indexKey ByteString) error {
	delete(akv.Index, string(indexKey))
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(akv.Index)
	if err != nil {
		return err
	}
	akv.Index = make(map[string]uint64)
	akv.Insert(indexKey, b.Bytes())
	return nil
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

	IndexKey := []byte("+index")
	switch action {
	case "get":
		indexAsBytes, err := store.Get(IndexKey)
		if err != nil {
			log.Fatal(err)
		}
		b := bytes.NewBuffer(indexAsBytes)
		d := gob.NewDecoder(b)
		var index map[string]uint64
		err = d.Decode(&index)
		if err != nil {
			log.Fatal(err)
		}
		if pos, ok := index[key]; !ok {
			log.Fatalf("Couldn't find key: %s", key)
		} else {
			kv, err := store.GetAt(uint64(pos))
			if err != nil {
				log.Fatalf("%s not found: %s\n", key, err.Error())
			}
			fmt.Printf("Value: %s\n", kv.Value)
		}
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
		storeIndexOnDisk(store, IndexKey)
	case "update":
		if len(values) < 4 {
			fmt.Println(Usage)
			os.Exit(1)
		}
		err := store.Update(ByteString(key), ByteString(maybeValue))
		if err != nil {
			log.Fatalf("Failed to update %s, %s\n", key, maybeValue)
		}
		storeIndexOnDisk(store, IndexKey)
	default:
		fmt.Println(Usage)
	}
}
