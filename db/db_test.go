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

func setKey(t *testing.T, d *db.Database, key, value string) {
	t.Helper()

	if err := d.SetKey(key, []byte(value)); err != nil {
		t.Fatalf("SetKey(%q, %q) failed: %v", key, value, err)
	}
}

func getKey(t *testing.T, d *db.Database, key string) string {
	t.Helper()

	value, err := d.GetKey(key)
	if err != nil {
		t.Fatalf("GetKey(%q) failed: %v", key, err)
	}

	return string(value)
}

func TestDeleteExtraKeys(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "kvdb")
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

	setKey(t, db, "utm", "utm-value")
	setKey(t, db, "fcim", "fcim-value")

	if err := db.DeleteExtraKeys(func(name string) bool { return name == "fcim" }); err != nil {
		t.Fatalf("Could not delete extra keys: %v", err)
	}

	if value := getKey(t, db, "utm"); value != "utm-value" {
		t.Errorf(`Unexpected value for key "utm": got %q, want %q`, value, "utm-value")
	}

	if value := getKey(t, db, "fcim"); value != "" {
		t.Errorf(`Unexpected value for key "us": got %q, want %q`, value, "")
	}
}
