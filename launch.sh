#!/bin/bash
set -e

trap 'killall distributed-store' SIGINT

cd $(dirname $0)

killall distributed-store || true
sleep 0.1

go install -v

distributed-store -db-location=chisinau.db -http-addr=127.0.0.2:8080 -config-file=sharding.toml -shard=Chisinau &
distributed-store -db-location=chisinau-r.db -http-addr=127.0.0.22:8080 -config-file=sharding.toml -shard=Chisinau -replica &

distributed-store -db-location=balti.db -http-addr=127.0.0.3:8080 -config-file=sharding.toml -shard=Balti &
distributed-store -db-location=balti-r.db -http-addr=127.0.0.33:8080 -config-file=sharding.toml -shard=Balti -replica &

distributed-store -db-location=orhei.db -http-addr=127.0.0.4:8080 -config-file=sharding.toml -shard=Orhei  &
distributed-store -db-location=orhei-r.db -http-addr=127.0.0.44:8080 -config-file=sharding.toml -shard=Orhei -replica &

distributed-store -db-location=cahul.db -http-addr=127.0.0.5:8080 -config-file=sharding.toml -shard=Cahul &
distributed-store -db-location=cahul-r.db -http-addr=127.0.0.55:8080 -config-file=sharding.toml -shard=Cahul -replica &

wait