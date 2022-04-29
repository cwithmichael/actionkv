package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	var (
		fname  string
		action string
		key    string
		value  string
	)

	usage := fmt.Sprintf(`Usage:
	%[1]s <file> get <key>
	%[1]s <file> delete <key>
	%[1]s <file> insert <key> <value>
	%[1]s <file> update <key> <value>
	`, os.Args[0])

	argValues := getArgs(usage)

	if len(argValues) == 3 {
		fname, action, key = argValues[0], argValues[1], argValues[2]
	} else if len(argValues) == 4 {
		fname, action, key, value = argValues[0], argValues[1], argValues[2], argValues[3]
	}

	IndexKey := []byte("+index")
	store := loadStore(fname)
	defer store.BackingFile.Close()

	switch strings.ToLower(action) {
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
		fmt.Printf("Deleted: %s\n", key)
		storeIndexOnDisk(store, IndexKey)
	case "insert":
		if len(argValues) < 4 {
			flag.Usage()
		}
		if err := store.Insert(ByteString(key), ByteString(value)); err != nil {
			log.Fatalf("Failed to insert %s, %s\n", key, value)
		}
		fmt.Printf("Inserted Key: %s Value: %s\n", key, value)
		storeIndexOnDisk(store, IndexKey)
	case "update":
		if len(argValues) < 4 {
			flag.Usage()
		}
		if err := store.Update(ByteString(key), ByteString(value)); err != nil {
			log.Fatalf("Failed to update %s, %s\n", key, value)
		}
		fmt.Printf("Updated Key: %s with Value: %s\n", key, value)
		storeIndexOnDisk(store, IndexKey)
	default:
		fmt.Println(usage)
	}
}

func getArgs(usage string) []string {
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(1)
	}
	if flag.NArg() < 3 {
		flag.Usage()
	}
	return flag.Args()
}

func loadStore(fname string) *ActionKV {
	store, err := NewActionKV(fname)
	if err != nil {
		log.Fatalf("Unable to open file: %s", fname)
	}
	if err = store.Load(); err != nil {
		log.Fatalf("Unable to load data: %s\n", err.Error())
	}
	return store
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
