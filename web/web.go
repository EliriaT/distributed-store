package web

import (
	"fmt"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/db/sharding"
	"log"
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
	errCh := make(chan error)
	//here it is different ,first get then redirect. But if its a replica, get should be first??. p.s. how to know it is a replica
	if keyShard != s.shards.CurrIdx {
		s.redirect(keyShard, w, r, errCh)
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
		log.Printf("Replicated on current shard = %d, error = %v, \n", s.shards.CurrIdx, err)
		return
	}

	shards, err := s.sharder.GetNReplicas(key, s.replicationFactor)
	if err != nil {
		log.Printf("Shards = %v, current shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(s.consistencyLevel)
	errCh := make(chan error)

	for i, shard := range shards {
		var closure func(shard int, errCh chan<- error)

		if shard == s.shards.CurrIdx {
			closure = func(shard int, errCh chan<- error) {
				log.Printf("Replicated on current shard = %d, error = %v, \n", s.shards.CurrIdx, err)
				// TODO Simulate here error
				err = s.db.SetKey(key, []byte(value))
				if err != nil {
					errCh <- err
				}
			}
		} else {
			closure = func(shard int, errCh chan<- error) {
				s.redirect(shard, w, r, errCh)
			}
		}

		if i < s.consistencyLevel { // aici tot posibil race condition??
			go func(shard int, errCh chan<- error) {
				closure(shard, errCh)
				defer wg.Done()
			}(shard, errCh)
		} else {
			go closure(shard, errCh)
		}
	}
	wg.Wait()

	fmt.Fprintf(w, "Shards = %v, current shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
	log.Println("\n\n\n\n-------------------------\n\n\n\n")
}

func (s *Server) redirect(shardIndx int, w http.ResponseWriter, r *http.Request, errCh chan<- error) {
	url := "http://" + s.shards.Addrs[shardIndx] + r.RequestURI + "&coordinator=false"
	//log.Printf("redirecting from shard %d to shard %d (%q)\n", s.shards.CurrIdx, shardIndx, url)

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error on node %d redirecting the request: %v \n", s.shards.CurrIdx, err)
		errCh <- err
		return
	}
	defer resp.Body.Close()

	//log.Println(w)
	//log.Println(resp.Body)
	//io.Copy(w, resp.Body)
	//buf := make([]byte, 8)
	//if _, err := io.CopyBuffer(w, resp.Body, buf); err != nil {
	//	// handle the error
	//	log.Println(err)
	//	return
	//}
	//fmt.Fprintf(w, "%s", resp.Body)
}

// DeleteExtraKeysHandler deletes keys that don't belong to the current shard.
func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v\n", s.db.DeleteExtraKeys(func(key string) bool {
		return s.sharder.Index(key) != s.shards.CurrIdx
	}))
}
