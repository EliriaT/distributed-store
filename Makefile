test:
	go test ./...

launch:
	./launch.sh

set:
	curl 'http://127.0.0.2:8080/set?key=utm&value=fcim'

get:
	curl 'http://127.0.0.2:8080/get?key=utm'

proto:
	protoc ./coordinator/grpc/proto/commands.proto --go_out=. --go-grpc_out=. --go-grpc_opt=require_unimplemented_servers=false

startSampleNode:
	distributed-store -db-location=database/chisinau -http-addr=127.0.0.0:8080 -config-file=sharding.toml -shard=Chisinau -env=config/env/.env0