package sharding

import (
	"github.com/EliriaT/distributed-store/config"
	"github.com/cespare/xxhash"
)
import "github.com/buraksezer/consistent"

type Sharder interface {
	Index(key string) int
}

type ConsistentHasher struct {
	ring   *consistent.Consistent
	config config.Config
}

func (c ConsistentHasher) Index(key string) int {
	member := c.ring.LocateKey([]byte(key))
	return c.config.GetShardIndex(member.String())
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

	return ConsistentHasher{ring: consistent.New(members, cfg), config: config}
}

type hasher struct{}

func (h hasher) Sum64(key []byte) uint64 {
	return xxhash.Sum64(key)
}
