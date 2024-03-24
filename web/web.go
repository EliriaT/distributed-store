package web

import (
	"fmt"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/db"
	"github.com/EliriaT/distributed-store/db/sharding"
	"golang.org/x/exp/slices"
	"io"
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
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
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
		log.Printf("Shards = %v, current shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
		return
	}

	var value []byte
	var replica int
	var response string

	if slices.Contains(shards, s.shards.CurrIdx) {
		replica = s.shards.CurrIdx
		value, err = s.db.GetKey(key)
		if err == nil {
			fmt.Fprintf(w, "Shard = %d, current shard = %d, current addr = %q, Value = %q, error = %v \n", replica, s.shards.CurrIdx, s.shards.Addrs[s.shards.CurrIdx], value, err)
			return
		}
	} else {
		for _, shard := range shards {
			replica = shard
			response, err = s.redirect(replica, w, r)
			if err != nil {
				continue
			}
			parts := strings.Split(response, ":")
			value = []byte(parts[1])
			break

		}
	}

	fmt.Fprintf(w, "Shard = %d, current shard = %d, current addr = %q, Value = %q, error = %v \n", replica, s.shards.CurrIdx, s.shards.Addrs[s.shards.CurrIdx], value, err)
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
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	shards, err := s.sharder.GetNReplicas(key, s.replicationFactor)
	if err != nil {
		log.Printf("Shards = %v, current shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(s.consistencyLevel)
	errCh := make(chan error, s.replicationFactor)

	for i, shard := range shards {
		var closure func(shard int, errCh chan<- error)

		if shard == s.shards.CurrIdx {
			closure = func(shard int, errCh chan<- error) {
				log.Printf("Replicated on current shard = %d, error = %v, \n", s.shards.CurrIdx, err)

				err = s.db.SetKey(key, []byte(value))
				if err != nil {
					errCh <- err
				}
			}
		} else {
			closure = func(shard int, errCh chan<- error) {
				_, err = s.redirect(shard, w, r)
				if err != nil {
					errCh <- err
				}
			}
		}
		// consistency level ar trebuie sa se mareasca doar la primirea de raspuns cu success. refactor with channels and for loop
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

	select {
	case err = <-errCh:
		fmt.Fprintf(w, "Shards = %v, current shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)
	default:
		fmt.Fprintf(w, "Shards = %v, current shard = %d, error = %v, \n", shards, s.shards.CurrIdx, err)

	}

	log.Println("\n\n\n\n-------------------------\n\n\n\n")
}

func (s *Server) redirect(shardIndx int, w http.ResponseWriter, r *http.Request) (string, error) {
	url := "http://" + s.shards.Addrs[shardIndx] + r.RequestURI + "&coordinator=false"

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error on node %d redirecting the request: %v \n", s.shards.CurrIdx, err)
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
