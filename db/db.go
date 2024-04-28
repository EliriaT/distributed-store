package db

type Database interface {
	SetKey(key string, value []byte) error
	GetKey(key string) ([]byte, error)
	DeleteExtraKeys(isExtra func(string) bool) error
}
