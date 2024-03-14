# distributed-key-value-store

To compile and install the package:
` go install -v`

To run it:
`distributed-store -db-location=$PWD/my.db -config-file=$PWD/sharding.toml -shard Chisinau`

`go run main.go -db-location my.db -shard Chisinau`

`curl http://127.0.0.1:8080/set?key=m&value=s`
`curl http://127.0.0.1:8080/get?key=m`