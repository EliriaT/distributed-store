package rest

import (
	"fmt"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/replication"
	"github.com/EliriaT/distributed-store/sharding"
	"github.com/gookit/slog"
	"github.com/madalv/conalg/caesar"
	"golang.org/x/exp/slices"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// HTTPServer contains HTTP method handlers to be used for the database.
type HTTPServer struct {
	db                db.Database
	shards            *config.Shards
	sharder           sharding.Sharder
	replicator        *replication.OrderedReplicator
	replicationFactor int
	consistencyLevel  int
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values.
func NewServer(db db.Database, shards *config.Shards, cfg config.Config, envPath string) *HTTPServer {
	replicator := replication.NewOrderedReplicator(db, shards, cfg)
	conalg := caesar.InitConalgModule(replicator, envPath, slog.FatalLevel, false)
	replicator.SetConalgModule(conalg)

	return &HTTPServer{
		db:                db,
		shards:            shards,
		sharder:           sharding.NewConsistentHasher(cfg),
		replicationFactor: cfg.ReplicationFactor,
		consistencyLevel:  cfg.ConsistencyLevel,
		replicator:        replicator,
	}
}

// GetHandler handles read requests to the distributed database.
func (s *HTTPServer) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	isCoordinator := r.Form.Get("coordinator")

	if strings.ToLower(isCoordinator) == "false" {
		value, err := s.db.GetKey(key)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, fmt.Sprintf("%s:%s", key, string(value)))

		return
	}

	shards, err := s.sharder.GetNReplicas(key, s.replicationFactor)
	if err != nil {
		log.Printf("Shards = %v, coordinator shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
		return
	}

	var value []byte
	var replica int
	var response string

	if slices.Contains(shards, s.shards.CurrIdx) {
		replica = s.shards.CurrIdx
		value, err = s.db.GetKey(key)

		if err == nil {
			log.Printf("Get processed on coordinator node %d, key = %s, value = %s", s.shards.CurrIdx, key, value)
			fmt.Fprintf(w, "Replica shard = %d, coordinator shard = %d, current addr = %q, Value = %q, error = %v \n", replica, s.shards.CurrIdx, s.shards.Addrs[s.shards.CurrIdx], value, err)
			return
		}
	}

	for _, shard := range shards {
		replica = shard
		response, err = s.redirect(replica, w, r)
		if err != nil {
			continue
		}
		parts := strings.Split(response, ":")
		value = []byte(parts[1])
		log.Printf("Get processed on shard node %d, key = %s, value = %s", replica, key, value)
		break
	}

	fmt.Fprintf(w, "Replica shard = %d, coordinator shard = %d, current addr = %q, Value = %q, error = %v \n", replica, s.shards.CurrIdx, s.shards.Addrs[s.shards.CurrIdx], value, err)
}

// SetHandler handles write requests to the distributed database.
func (s *HTTPServer) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")
	isCoordinator := r.Form.Get("coordinator")

	// this method should be accessed only from the nodes itself. Should not be exposed publicly.
	if strings.ToLower(isCoordinator) == "false" {
		err := s.db.SetKey(key, []byte(value))
		log.Printf("Replicated on replica shard = %d, key = %s, value = %s, error = %v, \n", s.shards.CurrIdx, key, value, err)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// Add to the order replicator the set command
	s.replicator.Replicate(key, value)

	shards, err := s.sharder.GetNReplicas(key, s.replicationFactor)
	if err != nil {
		log.Printf("Shards = %v, coordinator shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(s.consistencyLevel)
	errCh := make(chan error, s.replicationFactor)
	successCh := make(chan int, s.replicationFactor)
	successCounter := 0
	errorCounter := 0

	for i, shard := range shards {
		var closure func(shard int, errCh chan<- error)

		if shard == s.shards.CurrIdx {
			// why we send errCh as fun arg ?
			closure = func(shard int, errCh chan<- error) {
				log.Printf("Replicated on coordinator replica shard = %d, key = %s, value = %s, error = %v, \n", s.shards.CurrIdx, key, value, err)

				err = s.db.SetKey(key, []byte(value))
				if err != nil {
					log.Printf("Failed to replicate key = %s, value = %s on shard %d, error = %v", key, value, shard, err)
					errCh <- err
					return
				}
				successCh <- shard
			}
		} else {
			closure = func(shard int, errCh chan<- error) {
				_, err := s.redirect(shard, w, r)
				if err != nil {
					log.Printf("Failed to replicate key = %s, value = %s on shard %d, error = %v", key, value, shard, err)
					errCh <- err
					return
				}
				successCh <- shard
			}
		}

		if i < s.consistencyLevel {
			go func(shard int, errCh chan<- error) {
				closure(shard, errCh)
				defer wg.Done()
			}(shard, errCh)
		} else {
			go closure(shard, errCh)
		}
	}

	wg.Wait()

	shards = make([]int, 0)

outerLoop:
	for {
		select {
		case replicatedShard := <-successCh:
			shards = append(shards, replicatedShard)
			successCounter++
			if successCounter == s.consistencyLevel {
				break outerLoop
			}

			// if s.replicationFactor responses arrive, then the wait is stopped.
			// TODO what if response does not arrive? rest timeout will happen ?
			if errorCounter+successCounter >= s.replicationFactor {
				break outerLoop
			}
		default:
			err = <-errCh
			errorCounter++
			if errorCounter+successCounter >= s.replicationFactor {
				break outerLoop
			}
		}
	}

	if err != nil && len(shards) != s.consistencyLevel {
		w.WriteHeader(http.StatusFailedDependency)
	}

	fmt.Fprintf(w, "CL = %d, RF = %d, Replicated successfully on shards = %v, coordinator shard = %d, error = %v, \n", s.consistencyLevel, s.replicationFactor, shards, s.shards.CurrIdx, err)

	log.Println("\n-------------------------")
}

func (s *HTTPServer) redirect(shardIndx int, w http.ResponseWriter, r *http.Request) (string, error) {
	url := "http://" + s.shards.Addrs[shardIndx] + r.RequestURI + "&coordinator=false"

	client := http.Client{
		Timeout: time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Error on node %d when redirecting the request: %v \n", s.shards.CurrIdx, err)
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("could not receive a success response on redirect")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error:", err)
		return "", err
	}

	return string(body), nil
}

// DeleteExtraKeysHandler deletes keys that don't belong to the current shard.
func (s *HTTPServer) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v\n", s.db.DeleteExtraKeys(func(key string) bool {
		return s.sharder.Index(key) != s.shards.CurrIdx
	}))
}
