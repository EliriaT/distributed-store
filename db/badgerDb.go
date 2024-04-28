package db

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"log"
	"time"
)

type BadgerDatabase struct {
	db *badger.DB
}

func NewBadgerDatabase(dbPath string) (db *BadgerDatabase, closeFunc func() error, err error) {
	badgerDb, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		log.Fatal(err)
	}

	db = &BadgerDatabase{
		db: badgerDb,
	}
	closeFunc = badgerDb.Close

	// garbage collection once in a while
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
		again:
			err := badgerDb.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}()

	return
}

func (d *BadgerDatabase) SetKey(key string, value []byte) error {
	return d.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), value)
		return err
	})
}

func (d *BadgerDatabase) GetKey(key string) ([]byte, error) {
	var result []byte

	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))

		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		result, err = item.ValueCopy(nil)

		return err
	})

	return result, err
}

func (d *BadgerDatabase) DeleteExtraKeys(isExtra func(string) bool) error {
	var keys []string

	err := d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if isExtra(key) {
				keys = append(keys, key)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return d.db.Update(func(txn *badger.Txn) error {
		for _, k := range keys {
			if err = txn.Delete([]byte(k)); err != nil {
				if errors.Is(badger.ErrTxnTooBig, err) {
					_ = txn.Commit()
					txn = d.db.NewTransaction(true)
					_ = txn.Delete([]byte(k))
					continue
				}
				return err
			}
		}
		return nil
	})
}
