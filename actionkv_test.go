package main

import (
	"os"
	"testing"
)

var (
	testKey   = ByteString("3")
	testValue = ByteString("2")
)

func TestInsertAndGet(t *testing.T) {
	store, err := NewActionKV("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer os.Remove("test")
	if err = store.Insert(testKey, testValue); err != nil {
		t.Fatalf(err.Error())
	}
	val, err := store.Get(testKey)
	if err != nil || val[0] != testValue[0] {
		t.Fatalf("Wanted %s, but got %s", testValue, val)
	}
}

func BenchmarkInsert(b *testing.B) {
	store, err := NewActionKV("test")
	if err != nil {
		b.Fatal(err.Error())
	}
	defer os.Remove("test")
	for n := 0; n < b.N; n++ {
		store.Insert(testKey, testValue)
	}
}
