package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"os"
)

// KeyValuePair represents a key-value pair
type KeyValuePair struct {
	Key   ByteString
	Value ByteString
}

// ActionKV represents the data store with a BackingFile and an Index
type ActionKV struct {
	BackingFile *os.File
	Index       map[string]uint64
}

// ByteString is just a slice of bytes
type ByteString []byte

// NewActionKV creates a new instance of ActionKV
func NewActionKV(fname string) (*ActionKV, error) {
	// can't defer file close here
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 6644)
	if err != nil {
		return nil, err
	}
	return &ActionKV{BackingFile: f, Index: make(map[string]uint64)}, nil
}

func processRecord(f io.Reader) (*KeyValuePair, error) {
	var (
		savedChecksum uint32
		keyLen        uint32
		valLen        uint32
	)
	if err := binary.Read(f, binary.LittleEndian, &savedChecksum); err != nil {
		return nil, err
	}
	if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(f, binary.LittleEndian, &valLen); err != nil {
		return nil, err
	}

	data := make([]byte, keyLen+valLen)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}

	checksum := crc32.ChecksumIEEE(data)
	if checksum != savedChecksum {
		log.Panicf("Invalid data: %x != %x", savedChecksum, checksum)
	}

	return &KeyValuePair{Key: data[0:keyLen], Value: data[keyLen:]}, nil
}

// Load loads the backing file and gets the currentPosition in the file
// to start adding data
func (a *ActionKV) Load() error {
	buf := bytes.Buffer{}
	if _, err := io.Copy(&buf, a.BackingFile); err != nil {
		log.Fatal(err)
	}
	f := bytes.NewReader(buf.Bytes())
	for {
		currentPosition, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		kv, err := processRecord(f)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		a.Index[string(kv.Key)] = uint64(currentPosition)
	}
	return nil
}

// GetAt gets a key-value pair at a certain position in the backing file
func (a *ActionKV) GetAt(pos uint64) (*KeyValuePair, error) {
	a.BackingFile.Seek(int64(pos), io.SeekStart)
	kv, err := processRecord(a.BackingFile)
	if err != nil {
		return nil, err
	}
	return kv, nil
}

// Get returns a value from the data store for a given key
func (a *ActionKV) Get(key ByteString) (ByteString, error) {
	pos, ok := a.Index[string(key)]
	if !ok {
		return nil, errors.New("couldn't find key")
	}
	kv, err := a.GetAt(pos)
	if err != nil {
		return nil, err
	}
	return kv.Value, nil
}

// Insert adds a new key-value pair to the data store
func (a *ActionKV) Insert(key ByteString, value ByteString) error {
	pos, err := a.InsertButIgnoreIndex(key, value)
	if err != nil {
		return err
	}
	a.Index[string(key)] = pos
	return nil
}

// Delete removes a key-value pair
func (a *ActionKV) Delete(key ByteString) error {
	if err := a.Insert(key, ByteString("value deleted")); err != nil {
		return err
	}
	return nil
}

// Update updates a key-value pair
func (a *ActionKV) Update(key ByteString, value ByteString) error {
	if err := a.Insert(key, value); err != nil {
		return err
	}
	return nil
}

// InsertButIgnoreIndex inserts a new key-value pair without using the index
func (a *ActionKV) InsertButIgnoreIndex(key ByteString, value ByteString) (uint64, error) {
	keyLen := len(key)
	valLen := len(value)
	tmp := make(ByteString, 0, keyLen+valLen)

	for _, b := range key {
		tmp = append(tmp, byte(b))
	}
	for _, b := range value {
		tmp = append(tmp, byte(b))
	}
	checksum := crc32.ChecksumIEEE(tmp)
	currentPosition, err := a.BackingFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if _, err = a.BackingFile.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}
	if err = binary.Write(a.BackingFile, binary.LittleEndian, checksum); err != nil {
		return 0, err
	}
	if err = binary.Write(a.BackingFile, binary.LittleEndian, uint32(keyLen)); err != nil {
		return 0, err
	}
	if err = binary.Write(a.BackingFile, binary.LittleEndian, uint32(valLen)); err != nil {
		return 0, err
	}
	if _, err = a.BackingFile.Write(tmp); err != nil {
		return 0, err
	}
	return uint64(currentPosition), nil
}
