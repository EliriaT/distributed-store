package replication

import (
	"encoding/json"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/sharding"
	"github.com/madalv/conalg/caesar"
	"golang.org/x/exp/slices"
	"log"
)

// OrderedReplicator uses the caesar consensus module for guaranteeing an order for set replicated commands
type OrderedReplicator struct {
	conalg            caesar.Conalg
	db                db.Database
	shards            *config.Shards
	sharder           sharding.Sharder
	batchQueue        []db.SetCommand
	maxBatchSize      uint8
	currBatchSize     int
	replicationFactor int
}

func (r *OrderedReplicator) DetermineConflict(c1, c2 []byte) bool {
	var command1, command2 db.SetCommand
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
	var command db.SetCommand
	err := json.Unmarshal(c, &command)
	if err != nil {
		return
	}

	shards, err := r.sharder.GetNReplicas(command.Key, r.replicationFactor)
	if err != nil {
		return
	}

	if slices.Contains(shards, r.shards.CurrIdx) {
		log.Printf("On Node %d, added to ordered queue command SET key = %s, value = %s", r.shards.CurrIdx, command.Key, command.Value)
		r.batchQueue = append(r.batchQueue, command)
		r.currBatchSize++

		if r.currBatchSize >= int(r.maxBatchSize) {
			err = r.db.WriteInBatch(r.batchQueue)
			if err == nil {
				log.Printf("On Node %d, succesfully syncronised batch %v", r.shards.CurrIdx, r.batchQueue)
				r.batchQueue = make([]db.SetCommand, 0, r.maxBatchSize)
				r.currBatchSize = 0
			}
		}
	}
}

func (s *OrderedReplicator) SetConalgModule(m caesar.Conalg) {
	s.conalg = m
}

func (s *OrderedReplicator) Replicate(key string, value string) {
	command := db.SetCommand{
		Key:   key,
		Value: value,
	}
	payload, _ := json.Marshal(command)

	s.conalg.Propose(payload)
}

func NewOrderedReplicator(datastore db.Database, shards *config.Shards, cfg config.Config) OrderedReplicator {
	batchSize := 100
	return OrderedReplicator{
		db:                datastore,
		shards:            shards,
		sharder:           sharding.NewConsistentHasher(cfg),
		maxBatchSize:      uint8(batchSize),
		currBatchSize:     0,
		batchQueue:        make([]db.SetCommand, 0, batchSize),
		replicationFactor: cfg.ReplicationFactor,
	}
}
