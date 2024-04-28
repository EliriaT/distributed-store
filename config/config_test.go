package config_test

import (
	"github.com/EliriaT/distributed-store/config"
	"os"
	"reflect"
	"testing"
)

func createConfig(t *testing.T, contents string) config.Config {
	t.Helper()

	f, err := os.CreateTemp(os.TempDir(), "config.toml")

	if err != nil {
		t.Fatalf("Couldn't create a temp file: %v", err)
	}
	defer f.Close()

	name := f.Name()
	defer os.Remove(name)

	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatalf("Could not write the config contents: %v", err)
	}

	configuration, err := config.ParseFile(name)
	if err != nil {
		t.Fatalf("Could not parse config: %v", err)
	}

	return configuration
}

func TestConfigParse(t *testing.T) {
	got := createConfig(t, `replication_factor = 1
		consistency_level = 1
		[[shards]]
		name = "Orhei"
		idx = 0
		address = "localhost:8080"`)

	want := config.Config{
		ReplicationFactor: 1,
		ConsistencyLevel:  1,
		Shards: []config.Shard{
			{
				Name:    "Orhei",
				Idx:     0,
				Address: "localhost:8080",
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("The config does not match: got: %#v, want: %#v", got, want)
	}
}

func TestParseShards(t *testing.T) {
	c := createConfig(t, `replication_factor = 2
	consistency_level = 1
	[[shards]]
		name = "Orhei"
		idx = 0
		address = "localhost:8080"
	[[shards]]
		name = "Chisinau"
		idx = 1
		address = "localhost:8081"`)

	got, err := config.ParseShards(c.Shards, "Chisinau")
	if err != nil {
		t.Fatalf("Could not parse shards %#v: %v", c.Shards, err)
	}

	want := &config.Shards{
		Count:   2,
		CurrIdx: 1,
		Addrs: map[int]string{
			0: "localhost:8080",
			1: "localhost:8081",
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("The shards config does match: got: %#v, want: %#v", got, want)
	}
}
