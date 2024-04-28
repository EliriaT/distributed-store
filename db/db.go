package db

type SetCommand struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type Database interface {
	SetKey(key string, value []byte) error
	GetKey(key string) ([]byte, error)
	DeleteExtraKeys(isExtra func(string) bool) error
	WriteInBatch(setCommands []SetCommand) error
}
