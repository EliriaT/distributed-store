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
	"sync"
	"time"
)

// GrpcServer uses grpc for node communication.
type GrpcServer struct {
	db                db.Database
	shards            *config.Shards
	sharder           sharding.Sharder
	replicator        *replication.OrderedReplicator
	replicationFactor int
	consistencyLevel  int
	PeerConnections   map[int]proto.NodeServiceClient
	proto.UnimplementedNodeServiceServer
}

func NewServer(db db.Database, shards *config.Shards, cfg config.Config, envPath string) *GrpcServer {
	replicator := replication.NewOrderedReplicator(db, shards, cfg)
	conalg := caesar.InitConalgModule(replicator, envPath, slog.FatalLevel, false)
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
		if g.shards.CurrIdx != 0 {
			time.Sleep(time.Minute)
		}
		value, err := g.db.GetKey(getCommand.Key)
		if err != nil {
			return &proto.GetResponse{
				Status: 500,
				Error:  fmt.Sprintf("Failed to write to db the key %s, error: %v", getCommand.Key, err),
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
			log.Printf("Get processed on coordinator node %d, key = %s, value = %s", g.shards.CurrIdx, getCommand.Key, string(value))
			return &proto.GetResponse{
				Status: 200,
				Value:  string(value),
				Error:  "",
			}, nil
		}
	}

	for _, shard := range shards {
		getCommand.Coordinator = false

		ctx2, cancelFunc := context.WithTimeout(context.Background(), time.Second)
		defer cancelFunc()

		response, err = g.PeerConnections[shard].Get(ctx2, getCommand)
		if err != nil {
			continue
		}

		log.Printf("Get processed on shard node %d, key = %s, value = %s", shard, getCommand.Key, string(value))
		return &proto.GetResponse{
			Status: 200,
			Value:  response.Value,
			Error:  "",
		}, nil
	}

	return &proto.GetResponse{
		Status: 424,
		Value:  fmt.Sprintf("Failed to get succesfully key %s from all replicas, status: %d, error: %v", getCommand.Key, 424, err),
		Error:  "",
	}, nil
}

func (g *GrpcServer) Set(ctx context.Context, setCommand *proto.SetRequest) (response *proto.SetResponse, err error) {
	key := setCommand.Key
	value := setCommand.Value

	if setCommand.Coordinator == false {
		if g.shards.CurrIdx != 0 {
			time.Sleep(time.Minute)
		}

		err = g.db.SetKey(key, []byte(value))
		if err != nil {
			return &proto.SetResponse{
				Status: 500,
				Error:  fmt.Sprintf("Failed to write to db the key %s, error: %v", key, err),
			}, err
		}

		return &proto.SetResponse{
			Status: 200,
			Error:  "",
		}, nil
	}

	// Add to the order replicator the set command
	g.replicator.Replicate(key, value)

	shards, err := g.sharder.GetNReplicas(key, g.replicationFactor)
	if err != nil {
		return &proto.SetResponse{
			Status: 500,
			Error:  fmt.Sprintf("Failed to get %d replicas for key %s", g.replicationFactor, key),
		}, err
	}

	var wg sync.WaitGroup
	wg.Add(g.consistencyLevel)
	errCh := make(chan error, g.replicationFactor)
	successCh := make(chan int, g.replicationFactor)
	successCounter := 0
	errorCounter := 0

	for i, shard := range shards {
		var closure func(shard int, errCh chan<- error)

		if shard == g.shards.CurrIdx {
			closure = func(shard int, errCh chan<- error) {
				err = g.db.SetKey(key, []byte(value))
				if err != nil {
					errCh <- err
					return
				}
				successCh <- shard
			}
		} else {
			closure = func(shard int, errCh chan<- error) {
				setCommand.Coordinator = false

				ctx2, cancelFunc := context.WithTimeout(context.Background(), time.Second)
				defer cancelFunc()

				_, err = g.PeerConnections[shard].Set(ctx2, setCommand)
				if err != nil {
					errCh <- err
					return
				}
				successCh <- shard
			}
		}

		if i < g.consistencyLevel {
			go func(shard int, errCh chan<- error) {
				closure(shard, errCh)
				defer wg.Done()
			}(shard, errCh)
		} else {
			go closure(shard, errCh)
		}
	}

	wg.Wait()

	replicatedOn := make([]int32, 0)

outerLoop:
	for {
		select {
		case replicatedShard := <-successCh:
			replicatedOn = append(replicatedOn, int32(replicatedShard))
			successCounter++
			if successCounter == g.consistencyLevel {
				break outerLoop
			}

			if errorCounter+successCounter >= g.replicationFactor {
				break outerLoop
			}
		default:
			<-errCh
			errorCounter++
			if errorCounter+successCounter >= g.replicationFactor {
				break outerLoop
			}
		}
	}

	status := 200
	if err != nil && len(replicatedOn) != g.consistencyLevel {
		status = 424
	}

	errorMessage := ""
	if err != nil {
		errorMessage = fmt.Sprintf("While replicating encounted error: %v", err)
	}

	return &proto.SetResponse{
		Status:       int32(status),
		ReplicatedOn: replicatedOn,
		Error:        errorMessage,
	}, nil
}

func (g *GrpcServer) DeleteExtraKeys(ctx context.Context, _ *proto.Empty) (*proto.StatusResponse, error) {
	err := g.db.DeleteExtraKeys(func(key string) bool {
		return g.sharder.Index(key) != g.shards.CurrIdx
	})

	status := 200
	errorMessage := ""
	if err != nil {
		status = 500
		errorMessage = fmt.Sprintf("Failed to delete extra keys, error: %v", err)
	}

	return &proto.StatusResponse{
		Status: int32(status),
		Error:  errorMessage,
	}, err
}
