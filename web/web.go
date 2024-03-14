package web

import (
	"fmt"
	"github.com/EliriaT/distributed-store/config"
	"github.com/EliriaT/distributed-store/db"
	"io"
	"net/http"
)

// Server contains HTTP method handlers to be used for the database.
type Server struct {
	db     *db.Database
	shards *config.Shards
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values.
func NewServer(db *db.Database, shards *config.Shards) *Server {
	return &Server{
		db:     db,
		shards: shards,
	}
}

func (s *Server) redirect(shardIndx int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.shards.Addrs[shardIndx] + r.RequestURI
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

// GetHandler handles read requests from the database.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	keyShard := s.shards.Index(key)

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

	shard := s.shards.Index(key)
	if shard != s.shards.CurrIdx {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))
	fmt.Fprintf(w, "Shard = %d, current shard = %d, error = %v, \n", shard, s.shards.CurrIdx, err)
}
