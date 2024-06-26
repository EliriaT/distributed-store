package db_test

import (
	"bytes"
	"github.com/EliriaT/distributed-store/db"
	"os"
	"testing"
)

func createTempDb(t *testing.T, readOnly bool) *db.BoltDatabase {
	t.Helper()

	f, err := os.CreateTemp(os.TempDir(), "kvdb")
	if err != nil {
		t.Fatalf("Could not create temp file: %v", err)
	}
	name := f.Name()
	f.Close()

	t.Cleanup(func() { os.Remove(name) })

	db, closeFunc, err := db.NewBoltDatabase(name)
	if err != nil {
		t.Fatalf("Could not create a new database: %v", err)
	}
	t.Cleanup(func() { closeFunc() })

	return db
}

func TestGetSet(t *testing.T) {
	db := createTempDb(t, false)

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

func setKey(t *testing.T, d *db.BoltDatabase, key, value string) {
	t.Helper()

	if err := d.SetKey(key, []byte(value)); err != nil {
		t.Fatalf("SetKey(%q, %q) failed: %v", key, value, err)
	}
}

func getKey(t *testing.T, d *db.BoltDatabase, key string) string {
	t.Helper()

	value, err := d.GetKey(key)
	if err != nil {
		t.Fatalf("GetKey(%q) failed: %v", key, err)
	}

	return string(value)
}

func TestDeleteExtraKeys(t *testing.T) {
	db := createTempDb(t, false)

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
