package db_test

import (
	"bytes"
	"github.com/EliriaT/distributed-store/db"
	"os"
	"testing"
)

func TestGetSet(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "db")
	if err != nil {
		t.Fatalf("Could not create temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	db, closeFunc, err := db.NewDatabase(name)
	if err != nil {
		t.Fatalf("Could not create a new database: %v", err)
	}
	defer closeFunc()

	key := "utm"
	value := "md"
	if err := db.SetKey(key, []byte(value)); err != nil {
		t.Fatalf("Could not write key: %v", err)
	}

	receivedValue, err := db.GetKey(key)
	if err != nil {
		t.Fatalf(`Could not get the key "utm": %v`, err)
	}

	if !bytes.Equal(receivedValue, []byte(value)) {
		t.Errorf(`Unexpected value for key "utm": got %q, want %q`, value, key)
	}
}
