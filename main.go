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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

var (
	dbLocation = flag.String("db-location", "", "The path to the bolt db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard to run")
	env        = flag.String("env", "", "The path to env file for the consensus module")
)

var kacp = keepalive.ClientParameters{
	Time:                20 * time.Second, // send pings every 20 seconds if there is no activity
	Timeout:             2 * time.Second,  // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

var kaep = keepalive.EnforcementPolicy{
	MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
	PermitWithoutStream: true,            // Allow pings even when there are no active streams
}

var kasp = keepalive.ServerParameters{
	MaxConnectionIdle:     30 * time.Second, // If a client is idle for 30 seconds, send a GOAWAY
	MaxConnectionAge:      16 * time.Hour,   // If any connection is alive for more than 16 hour, send a GOAWAY
	MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
	Time:                  25 * time.Second, // Ping the client if it is idle for 25 seconds to ensure the connection is still active
	Timeout:               2 * time.Second,  // Wait 2 second for the ping ack before assuming the connection is dead
}

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
	//startHttpServer(database, shards, shardConfig)
}

func startGRPCServer(db db.Database, shards *config.Shards, cfg config.Config) {
	srv := grpcCoordinator.NewServer(db, shards, cfg, *env)

	nodeAddress := strings.Split(*httpAddr, ":")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", nodeAddress[1]))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	proto.RegisterNodeServiceServer(s, srv)

	// establishing http2 long live connections with peer nodes
	for _, peer := range cfg.Shards {
		if peer.Idx != shards.CurrIdx {
			address := strings.Split(peer.Address, ":")
			conn, err := grpc.NewClient(fmt.Sprintf(":%s", address[1]), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithKeepaliveParams(kacp))
			if err != nil {
				log.Fatalf("grpc: did not connect to node %s, error: %v", peer.Name, err)
			}
			defer conn.Close()

			peerConn := proto.NewNodeServiceClient(conn)
			srv.PeerConnections[peer.Idx] = peerConn
		}
	}

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
