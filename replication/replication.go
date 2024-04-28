package replication

import (
	"encoding/json"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/db/sharding"
	"github.com/madalv/conalg/caesar"
)

type SetCommand struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// OrderedReplicator uses the caesar consensus module for guaranteeing an order for set replicated commands
type OrderedReplicator struct {
	conalg  caesar.Conalg
	db      db.Database
	shards  *config.Shards
	sharder sharding.Sharder
}

func (r *OrderedReplicator) DetermineConflict(c1, c2 []byte) bool {
	var command1, command2 SetCommand
	err := json.Unmarshal(c1, &command1)
	if err != nil {
		return false
	}
	err = json.Unmarshal(c2, &command2)
	if err != nil {
		return false
	}

	return command1.Key == command2.Key
}

// use a batch for consistent replication
func (r *OrderedReplicator) Execute(c []byte) {
	var command SetCommand
	err := json.Unmarshal(c, &command)
	if err != nil {
		return
	}

	if r.sharder.Index(command.Key) == r.shards.CurrIdx {
		err = r.db.SetKey(command.Key, []byte(command.Value))
		if err != nil {
			return
		}
	}
}

func (s *OrderedReplicator) SetConalgModule(m caesar.Conalg) {
	s.conalg = m
}

func (s *OrderedReplicator) Replicate(key string, value string) {
	command := SetCommand{
		Key:   key,
		Value: value,
	}
	payload, _ := json.Marshal(command)

	s.conalg.Propose(payload)
}

func NewOrderedReplicator(db db.Database, shards *config.Shards, cfg config.Config) OrderedReplicator {
	return OrderedReplicator{
		db:      db,
		shards:  shards,
		sharder: sharding.NewConsistentHasher(cfg),
	}
}
