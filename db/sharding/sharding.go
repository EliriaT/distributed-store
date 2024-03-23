package sharding

import (
	"github.com/EliriaT/distributed-store/config"
	"github.com/cespare/xxhash"
)
import "github.com/buraksezer/consistent"

type Sharder interface {
	Index(key string) int
	GetNReplicas(key string, count int) ([]int, error)
}

type ConsistentHasher struct {
	ring   *consistent.Consistent
	config config.Config
}

func (c ConsistentHasher) Index(key string) int {
	member := c.ring.LocateKey([]byte(key))
	return c.config.GetShardIndex(member.String())
}

// GetNReplicas gets n replicas for a given key, one of which is the key owner.
func (c ConsistentHasher) GetNReplicas(key string, count int) ([]int, error) {
	members, err := c.ring.GetClosestN([]byte(key), count)

	if err != nil {
		return nil, err
	}
	membersIndexes := make([]int, len(members), len(members))

	for i, member := range members {
		membersIndexes[i] = c.config.GetShardIndex(member.String())
	}

	return membersIndexes, nil
}

func NewConsistentHasher(config config.Config) ConsistentHasher {
	var members []consistent.Member
	for _, shard := range config.Shards {
		members = append(members, shard)
	}

	cfg := consistent.Config{
		PartitionCount:    71,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	ring := consistent.New(members, cfg)

	return ConsistentHasher{ring: ring, config: config}
}

type hasher struct{}

func (h hasher) Sum64(key []byte) uint64 {
	return xxhash.Sum64(key)
}
