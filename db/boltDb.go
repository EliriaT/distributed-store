package db

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")

// BoltDatabase is a bolt database.
type BoltDatabase struct {
	db *bolt.DB
}

// NewBoltDatabase returns an instance of a database.
func NewBoltDatabase(dbPath string) (db *BoltDatabase, closeFunc func() error, err error) {
	boltDb, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &BoltDatabase{db: boltDb}
	closeFunc = boltDb.Close

	if err := db.createBuckets(); err != nil {
		// return closefunc instead
		closeFunc()
		return nil, nil, fmt.Errorf("creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

func (d *BoltDatabase) createBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		return nil
	})
}

// SetKey sets the key to the requested value into the default database or returns an error.
func (d *BoltDatabase) SetKey(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

// GetKey get the value of the requested from a default database.
func (d *BoltDatabase) GetKey(key string) ([]byte, error) {
	var result []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = copyByteSlice(b.Get([]byte(key)))
		return nil
	})

	if err == nil {
		return result, nil
	}
	return nil, err
}

func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}

// DeleteExtraKeys deletes the keys that do not belong to this shard.
func (d *BoltDatabase) DeleteExtraKeys(isExtra func(string) bool) error {
	var keys []string

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, v []byte) error {
			key := string(k)
			if isExtra(key) {
				keys = append(keys, key)
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)

		for _, k := range keys {
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *BoltDatabase) WriteInBatch(setCommands []SetCommand) error {
	err := d.db.Batch(func(tx *bolt.Tx) error {
		for _, command := range setCommands {
			err := tx.Bucket(defaultBucket).Put([]byte(command.Key), []byte(command.Value))
			if err != nil {
				return err
			}
		}

		return nil
	})

	return err
}
