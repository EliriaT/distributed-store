#!/bin/bash
set -e

trap 'killall distributed-store' SIGINT

cd $(dirname $0)

killall distributed-store || true
sleep 0.1

go install -v

distributed-store -db-location=chisinau.db -http-addr=127.0.0.1:8080 -config-file=sharding.toml -shard=Chisinau &
distributed-store -db-location=balti.db -http-addr=127.0.0.1:8081 -config-file=sharding.toml -shard=Balti &
distributed-store -db-location=orhei.db -http-addr=127.0.0.1:8082 -config-file=sharding.toml -shard=Orhei &

wait