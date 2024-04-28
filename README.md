# distributed-key-value-store

To compile and install the package:

` go install -v`

To run one sample node:

`distributed-store -db-location=$PWD/my.db -http-addr=127.0.0.2:8080 -config-file=$PWD/sharding.toml -shard Chisinau`

To run an example cluster you can run:

`launch.sh`

To run benchmarking, in the bench folder:

`go run main.go -iterations=1000 -concurrency=16 -read-iterations=1000`

To rebalance the key among the nodes, run for each node:

`curl 'http://127.0.0.1:8080/purge'`

Run tests:

`go test ./...`

`go run main.go -db-location my.db -shard Chisinau`

`curl 'http://127.0.0.2:8080/set?key=m&value=s'`
`curl 'http://127.0.0.2:8080/get?key=m'`

Index of shards should be consecutive!

db-location for badger db should be a path to a directory, for bold db a path to a file.