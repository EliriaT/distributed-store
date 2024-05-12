package grpc

import (
	"context"
	"fmt"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/coordinator/grpc/proto"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/replication"
	"github.com/EliriaT/distributed-store/sharding"
	"github.com/gookit/slog"
	"github.com/madalv/conalg/caesar"
	"log"
	"slices"
)

// GrpcServer uses grpc for node communication.
type GrpcServer struct {
	db                db.Database
	shards            *config.Shards
	sharder           sharding.Sharder
	replicator        replication.OrderedReplicator
	replicationFactor int
	consistencyLevel  int
	PeerConnections   map[int]proto.NodeServiceClient
	proto.UnimplementedNodeServiceServer
}

func NewServer(db db.Database, shards *config.Shards, cfg config.Config, envPath string) *GrpcServer {
	replicator := replication.NewOrderedReplicator(db, shards, cfg)
	conalg := caesar.InitConalgModule(&replicator, envPath, slog.FatalLevel, false)
	replicator.SetConalgModule(conalg)

	return &GrpcServer{
		db:                db,
		shards:            shards,
		sharder:           sharding.NewConsistentHasher(cfg),
		replicationFactor: cfg.ReplicationFactor,
		consistencyLevel:  cfg.ConsistencyLevel,
		replicator:        replicator,
		PeerConnections:   make(map[int]proto.NodeServiceClient),
	}
}

func (g *GrpcServer) Get(ctx context.Context, getCommand *proto.GetRequest) (response *proto.GetResponse, err error) {
	if getCommand.Coordinator == false {
		value, err := g.db.GetKey(getCommand.Key)
		if err != nil {
			return &proto.GetResponse{
				Status: 500,
				Error:  "Failed to read from db the key",
			}, err

		}
		return &proto.GetResponse{
			Status: 200,
			Value:  string(value),
			Error:  "",
		}, nil
	}

	shards, err := g.sharder.GetNReplicas(getCommand.Key, g.replicationFactor)
	if err != nil {
		return &proto.GetResponse{
			Status: 500,
			Error:  fmt.Sprintf("Failed to get %d replicas for key %s", g.replicationFactor, getCommand.Key),
		}, err
	}

	var value []byte

	if slices.Contains(shards, g.shards.CurrIdx) {
		value, err = g.db.GetKey(getCommand.Key)

		if err == nil {
			log.Printf("Get processed on coordinator node %d, key = %s, value = %s", g.shards.CurrIdx, getCommand.Key, value)
			return &proto.GetResponse{
				Status: 200,
				Value:  string(value),
				Error:  "",
			}, nil
		}
	}

	for _, shard := range shards {
		getCommand.Coordinator = false
		response, err = g.PeerConnections[shard].Get(ctx, getCommand)
		if err != nil {
			continue
		}

		log.Printf("Get processed on shard node %d, key = %s, value = %s", shard, getCommand.Key, value)
		return &proto.GetResponse{
			Status: 200,
			Value:  response.Value,
			Error:  "",
		}, nil
	}

	return &proto.GetResponse{
		Status: 500,
		Value:  fmt.Sprintf("Failed to get succesfully key %s from all replicas, status: %d, error: %v", getCommand.Key, response.Status, response.Error),
		Error:  "",
	}, nil
}

func (g *GrpcServer) Set(context.Context, *proto.SetRequest) (*proto.SetResponse, error) {

	return &proto.SetResponse{
		Status:       200,
		ReplicatedOn: []int32{1, 2, 3, 4},
		Error:        "",
	}, nil
}

func (g *GrpcServer) DeleteExtraKeys(context.Context, *proto.Empty) (*proto.StatusResponse, error) {

	return &proto.StatusResponse{
		Status: 200,
		Error:  "",
	}, nil
}
