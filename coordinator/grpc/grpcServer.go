package grpc

import (
	"context"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/coordinator/grpc/proto"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/replication"
	"github.com/EliriaT/distributed-store/sharding"
	"github.com/gookit/slog"
	"github.com/madalv/conalg/caesar"
)

// GrpcServer uses grpc for node communication.
type GrpcServer struct {
	db                db.Database
	shards            *config.Shards
	sharder           sharding.Sharder
	replicator        replication.OrderedReplicator
	replicationFactor int
	consistencyLevel  int
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
	}
}

func (g *GrpcServer) Get(context.Context, *proto.GetRequest) (*proto.GetResponse, error) {

	return &proto.GetResponse{
		Status: 200,
		Value:  "400",
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
