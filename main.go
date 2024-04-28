package main

import (
	"flag"
	config "github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/web"
	"log"
	"net/http"
)

var (
	dbLocation = flag.String("db-location", "", "The path to the bolt db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard to run")
	env        = flag.String("env", "", "The path to env file for the consensus module")
)

func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatalf("Must provide db-location")
	}

	if *shard == "" {
		log.Fatalf("Must provide shard")
	}
}

func main() {
	parseFlags()

	shardConfig, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config(%q): %v", *configFile, err)
	}

	shards, err := config.ParseShards(shardConfig.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards config: %v", err)
	}

	log.Printf("Shard count is %d, current shard: %d", shards.Count, shards.CurrIdx)

	database, closeFunc, err := db.NewBadgerDatabase(*dbLocation)
	if err != nil {
		log.Fatalf("Error creating %q: %v", *dbLocation, err)
	}
	defer closeFunc()

	srv := web.NewServer(database, shards, shardConfig, *env)

	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	// TODO adjust purge to take into account n replicas
	http.HandleFunc("/purge", srv.DeleteExtraKeysHandler)

	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}
