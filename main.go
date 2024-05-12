package main

import (
	"flag"
	"fmt"
	config "github.com/EliriaT/distributed-store/config"
	grpcCoordinator "github.com/EliriaT/distributed-store/coordinator/grpc"
	"github.com/EliriaT/distributed-store/coordinator/grpc/proto"
	"github.com/EliriaT/distributed-store/coordinator/rest"
	"github.com/EliriaT/distributed-store/db"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"strings"
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

	startGRPCServer(database, shards, shardConfig)
}

func startGRPCServer(db db.Database, shards *config.Shards, cfg config.Config) {
	srv := grpcCoordinator.NewServer(db, shards, cfg, *env)

	address := strings.Split(*httpAddr, ":")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", address[1]))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterNodeServiceServer(s, srv)

	if err = s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func startHttpServer(db db.Database, shards *config.Shards, cfg config.Config) {
	srv := rest.NewServer(db, shards, cfg, *env)

	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	// TODO adjust purge to take into account n replicas
	http.HandleFunc("/purge", srv.DeleteExtraKeysHandler)

	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}
