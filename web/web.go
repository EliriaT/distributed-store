package web

import (
	"encoding/json"
	"fmt"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/db/sharding"
	"github.com/EliriaT/distributed-store/replication"
	"io"
	"net/http"
	"strings"
	"sync"
)

// Server contains HTTP method handlers to be used for the database.
type Server struct {
	db                *db.Database
	shards            *config.Shards
	sharder           sharding.Sharder
	replicationFactor int
	consistencyLevel  int
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values.
func NewServer(db *db.Database, shards *config.Shards, cfg config.Config) *Server {
	return &Server{
		db:                db,
		shards:            shards,
		sharder:           sharding.NewConsistentHasher(cfg),
		replicationFactor: cfg.ReplicationFactor,
		consistencyLevel:  cfg.ConsistencyLevel,
	}
}

// GetHandler handles read requests from the database.
// TODO GET FROM REPLICA WITHOUT REDIRECT
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	keyShard := s.sharder.Index(key)
	//here it is different ,first get then redirect. But if its a replica, get should be first??. p.s. how to know it is a replica
	if keyShard != s.shards.CurrIdx {
		s.redirect(keyShard, w, r)
		return
	}

	value, err := s.db.GetKey(key)
	fmt.Fprintf(w, "Shard = %d, current shard = %d, current addr = %q, Value = %q, error = %v \n", keyShard, s.shards.CurrIdx, s.shards.Addrs[keyShard], value, err)
}

// SetHandler handles write requests from the database.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")
	isCoordinator := r.Form.Get("coordinator")

	// this method should be accessed only from the nodes itself. Should not be exposed publicly.
	if strings.ToLower(isCoordinator) == "false" {
		// the caesar will nevertheless guarantee an eventual consistency even if there are in unordered writes
		err := s.db.SetKey(key, []byte(value))
		fmt.Fprintf(w, "Replicated on current shard = %d, error = %v, \n", s.shards.CurrIdx, err)
		return
	}

	shards, err := s.sharder.GetNReplicas(key, s.replicationFactor)
	if err != nil {
		fmt.Fprintf(w, "Shards = %v, current shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
		return
	}

	var wg sync.WaitGroup

	wg.Add(s.consistencyLevel)

	for i, shard := range shards {
		var closure func(shard int)

		if shard == s.shards.CurrIdx {
			closure = func(shard int) {
				fmt.Fprintf(w, "Replicated on current shard = %d, error = %v, \n", s.shards.CurrIdx, err)
				// TODO Simulate here error
				err = s.db.SetKey(key, []byte(value))
			}
		} else {
			closure = func(shard int) {
				s.redirect(shard, w, r)
			}
		}

		if i < s.consistencyLevel {
			go func(shard int) {
				closure(shard)
				defer wg.Done()
			}(shard)
		} else {
			go closure(shard)
		}
	}
	wg.Wait()

	fmt.Fprintf(w, "Shards = %v, current shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
}

func replicateToNode(shard int) func(shard int) {

}

func (s *Server) redirect(shardIndx int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.shards.Addrs[shardIndx] + r.RequestURI + "&coordinator=false"
	fmt.Fprintf(w, "redirecting from shard %d to shard %d (%q)\n", s.shards.CurrIdx, shardIndx, url)

	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redirecting the request: %v \n", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

// DeleteExtraKeysHandler deletes keys that don't belong to the current shard.
func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v\n", s.db.DeleteExtraKeys(func(key string) bool {
		return s.sharder.Index(key) != s.shards.CurrIdx
	}))
}

// GetNextKeyForReplication returns the next key for replication.
func (s *Server) GetNextKeyForReplication(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	k, v, err := s.db.GetNextKeyForReplication()
	enc.Encode(&replication.NextKeyValue{
		Key:   string(k),
		Value: string(v),
		Err:   err,
	})
}

// DeleteReplicationKey deletes the key from replica queue.
func (s *Server) DeleteReplicationKey(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	key := r.Form.Get("key")
	value := r.Form.Get("value")

	err := s.db.DeleteReplicationKey([]byte(key), []byte(value))
	if err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
		fmt.Fprintf(w, "error: %v", err)
		return
	}

	fmt.Fprintf(w, "ok")
}
