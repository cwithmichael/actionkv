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

func main() {
	argValues := getArgs()
	fname, action, key := argValues[0], argValues[1], argValues[2]
	var maybeValue string
	if len(argValues) == 4 {
		maybeValue = argValues[3]
	}

	store, err := NewActionKV(fname)
	if err != nil {
		log.Fatalf("Unable to open file: %s", fname)
	}
	defer store.BackingFile.Close()
	if err = store.Load(); err != nil {
		log.Fatalf("Unable to load data: %s\n", err.Error())
	}

	IndexKey := []byte("+index")
	switch action {
	case "get":
		var index map[string]uint64
		indexAsBytes, err := store.Get(IndexKey)
		if err != nil {
			log.Fatal(err)
		}
		b := bytes.NewBuffer(indexAsBytes)
		if err = gob.NewDecoder(b).Decode(&index); err != nil {
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
		if err := store.Delete(ByteString(key)); err != nil {
			log.Fatalf("Failed to delete %s\n", key)
		}
		storeIndexOnDisk(store, IndexKey)
	case "insert":
		if len(argValues) < 4 {
			flag.Usage()
		}
		if err := store.Insert(ByteString(key), ByteString(maybeValue)); err != nil {
			log.Fatalf("Failed to insert %s, %s\n", key, maybeValue)
		}
		storeIndexOnDisk(store, IndexKey)
	case "update":
		if len(argValues) < 4 {
			flag.Usage()
		}
		if err := store.Update(ByteString(key), ByteString(maybeValue)); err != nil {
			log.Fatalf("Failed to update %s, %s\n", key, maybeValue)
		}
		storeIndexOnDisk(store, IndexKey)
	default:
		fmt.Println(Usage)
	}
}

func getArgs() []string {
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, Usage)
		os.Exit(1)
	}
	if flag.NArg() < 3 {
		flag.Usage()
	}
	return flag.Args()
}

func storeIndexOnDisk(akv *ActionKV, indexKey ByteString) error {
	delete(akv.Index, string(indexKey))
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(akv.Index); err != nil {
		return err
	}
	akv.Index = make(map[string]uint64)
	akv.Insert(indexKey, b.Bytes())
	return nil
}
