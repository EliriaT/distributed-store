# distributed-key-value-store

To compile and install the package:
` go install -v`

To run it:
`distributed-store -db-location=$PWD/my.db -config-file=$PWD/sharding.toml -shard Chisinau`

`go run main.go -db-location my.db -shard Chisinau`

`curl 'http://127.0.0.1:8080/set?key=m&value=s'`
`curl 'http://127.0.0.1:8080/get?key=m'`

To run benchmarking, in the bench folder:
`go run main.go -iterations=1000 -concurrency=256 -read-iterations=1000`

Run tests:
`go test ./...`

To rebalance the key among the nodes, run for each node:
`curl 'http://127.0.0.1:8080/purge'`

Index of shards should be consecutive!