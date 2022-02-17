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

type KeyValuePair struct {
	Key   ByteString
	Value ByteString
}

type ActionKV struct {
	BackingFile *os.File
	Index       map[string]uint64
}

type ByteString []byte

func NewActionKV(fname string) (*ActionKV, error) {
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 6644)
	if err != nil {
		return nil, err
	}
	// can't defer file close here
	return &ActionKV{BackingFile: f, Index: make(map[string]uint64)}, nil
}

func processRecord(f io.Reader) (*KeyValuePair, error) {
	var savedChecksum uint32
	err := binary.Read(f, binary.LittleEndian, &savedChecksum)
	if err != nil {
		return nil, err
	}
	var keyLen uint32
	err = binary.Read(f, binary.LittleEndian, &keyLen)
	if err != nil {
		return nil, err
	}
	var valLen uint32
	err = binary.Read(f, binary.LittleEndian, &valLen)
	if err != nil {
		return nil, err
	}
	dataLen := keyLen + valLen
	data := make([]byte, dataLen)
	_, err = io.ReadFull(f, data)
	if err != nil {
		return nil, err
	}
	checksum := crc32.ChecksumIEEE(data)
	if checksum != savedChecksum {
		log.Panicf("Invalid data: %x != %x", savedChecksum, checksum)
	}

	return &KeyValuePair{Key: data[0:keyLen], Value: data[keyLen:]}, nil
}

func (a *ActionKV) Load() error {
	buf := bytes.Buffer{}
	_, err := io.Copy(&buf, a.BackingFile)
	if err != nil {
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

func (a *ActionKV) GetAt(pos uint64) (*KeyValuePair, error) {
	a.BackingFile.Seek(int64(pos), io.SeekStart)
	kv, err := processRecord(a.BackingFile)
	if err != nil {
		return nil, err
	}
	return kv, nil
}

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

func (a *ActionKV) Insert(key ByteString, value ByteString) error {
	pos, err := a.InsertButIgnoreIndex(key, value)
	if err != nil {
		return err
	}
	a.Index[string(key)] = pos
	return nil
}

func (a *ActionKV) Delete(key ByteString) error {
	err := a.Insert(key, ByteString("value deleted"))
	if err != nil {
		return err
	}
	return nil
}

func (a *ActionKV) Update(key ByteString, value ByteString) error {
	err := a.Insert(key, value)
	if err != nil {
		return err
	}
	return nil
}

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
	_, err = a.BackingFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	err = binary.Write(a.BackingFile, binary.LittleEndian, checksum)
	if err != nil {
		return 0, err
	}
	err = binary.Write(a.BackingFile, binary.LittleEndian, uint32(keyLen))
	if err != nil {
		return 0, err
	}
	err = binary.Write(a.BackingFile, binary.LittleEndian, uint32(valLen))
	if err != nil {
		return 0, err
	}
	_, err = a.BackingFile.Write(tmp)
	if err != nil {
		return 0, err
	}
	return uint64(currentPosition), nil
}
