package config

import (
	"fmt"
	"github.com/BurntSushi/toml"
)

// Shard is a node responsible for a set of keys.
type Shard struct {
	Idx     int
	Name    string
	Address string
}

func (m Shard) String() string {
	return m.Name
}

// Config describes the sharding config.
type Config struct {
	Shards            []Shard
	ReplicationFactor int `toml:"replication_factor"`
	ConsistencyLevel  int `toml:"consistency_level"`
}

func (c Config) GetShardIndex(name string) int {
	for _, shard := range c.Shards {
		if shard.Name == name {
			return shard.Idx
		}
	}

	return 0
}

// Shards represents an easier-to-use representation of
// the sharding config: the shards count, current shard index and
// the addresses of all other shards too.
type Shards struct {
	Count   int
	CurrIdx int
	Addrs   map[int]string
}

// ParseFile parses the config, validates it and returns it upon success.
func ParseFile(filename string) (Config, error) {
	var c Config
	if _, err := toml.DecodeFile(filename, &c); err != nil {
		return Config{}, err
	}

	err := validateConfiguration(c)

	return c, err
}

func validateConfiguration(config Config) error {
	shardsCount := len(config.Shards)
	if config.ReplicationFactor > shardsCount {
		return fmt.Errorf("replication factor, %d, cannot be greater than the number of shards %d", config.ReplicationFactor, shardsCount)
	}

	if config.ConsistencyLevel > config.ReplicationFactor {
		return fmt.Errorf("consistency level, %d, cannot be greater than the replication factor %d", config.ConsistencyLevel, config.ReplicationFactor)
	}

	if config.ReplicationFactor < 1 {
		return fmt.Errorf("replication factor, %d, cannot be smaller than 1", config.ReplicationFactor)

	}

	if config.ConsistencyLevel < 1 {
		return fmt.Errorf("consistency level, %d, cannot be smaller than 1", config.ReplicationFactor)
	}

	return nil
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
