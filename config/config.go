package config

import (
	"fmt"
	"hash/fnv"
)

// Shard is a node with unique set of keys.
type Shard struct {
	Idx     int
	Name    string
	Address string
}

// Config describes the sharding config.
type Config struct {
	Shards []Shard
}

// Shards represents an easier-to-use representation of
// the sharding config: the shards count, current shard index and
// the addresses of all other shards too.
type Shards struct {
	Count   int
	CurrIdx int
	Addrs   map[int]string
}

// ParseShards converts and verifies the list of shards
// specified in the config into a form that can be used
// for routing.
func ParseShards(shards []Shard, currShardName string) (*Shards, error) {
	shardCount := len(shards)
	shardIdx := -1
	addrs := make(map[int]string)

	for _, s := range shards {
		if _, ok := addrs[s.Idx]; ok {
			return nil, fmt.Errorf("duplicate shard index: %d", s.Idx)
		}

		addrs[s.Idx] = s.Address
		if s.Name == currShardName {
			shardIdx = s.Idx
		}
	}

	for i := 0; i < shardCount; i++ {
		if _, ok := addrs[i]; !ok {
			return nil, fmt.Errorf("shard %d is not found", i)
		}
	}

	if shardIdx < 0 {
		return nil, fmt.Errorf("shard %q was not found", currShardName)
	}

	return &Shards{
		Addrs:   addrs,
		Count:   shardCount,
		CurrIdx: shardIdx,
	}, nil
}

// Index returns the shard number for the corresponding key.
func (s *Shards) Index(key string) int {
	h := fnv.New64()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.Count))
}
