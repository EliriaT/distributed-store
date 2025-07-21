# distributed-key-value-store

### About the project

In most distributed data storage systems, a leader based replication is used. However, this architecture introduces significant latency in a geo-distributed environment, especially when writes are directed only to a leader node situated at large distances. 

The aim of this study was to develop a distributed key-value data store with leaderless replication. This would allow write operations to be performed on the nearest geographical node, reducing latency caused by physical distance. Additionally, writing to any node in the distributed database would enable load balancing the write operations across all system nodes, ensuring a balanced traffic distribution. 

The system was implemented in the Go programming language and employs a leaderless consensus algorithm called Caesar to ensure data consistency. 

The final system can be configured to use either HTTP or gRPC communication protocol, and either B-tree or LSM (log-structured merge-tree) storage index.

Other system configurations, such as replication factor and consistency level, are also possible.

A benchmarking on a local computer was done in order to assess the performance of the implemented system. The system achieved a peak throughput of 14,000 read requests per second across three nodes. Under peak load, the maximum observed latency for read operations was 10 ms. 95% of read requests completed in under 1.58 ms, as monitored via Grafana dashboards. Write operations showed a higher latency, with 95% of write requests completing within 150 ms at a sustained load of 120 writes per second. The benchmarking on a local computer confirmed the system's capacity for high-throughput, low-latency operations, but the results are constrained by the non-distributed testing setup.

1. To compile and install the package:

` go install -v`

2. To run one sample node:

```
distributed-store
  -db-location=$PWD/my.db
  -http-addr=127.0.0.2:8080
  -config-file=$PWD/sharding.toml
  -shard Chisinau`
```

3. To run an example cluster you can run:

`launch.sh`

4. To run benchmarking, in the bench folder:

`go run main.go -iterations=1000 -concurrency=16 -read-iterations=1000`

5. To rebalance the key among the nodes, run for each node:

`curl 'http://127.0.0.1:8080/purge'`

6. Run tests:

`go test ./...`

------------------------------------------------------------------------
Other: 
 
`curl 'http://127.0.0.2:8080/set?key=utm&value=fcim'`
`curl 'http://127.0.0.2:8080/get?key=utm'`

`docker build -t node .`

`docker run --rm -p 8080:8080 node`

`docker compose up`

`docker compose -f stats.yaml up`

Index of shards should be consecutive!

db-location for badger db should be a path to a directory, for bold db a path to a file.
