#!/bin/bash
set -e

trap 'killall distributed-store' SIGINT

cd $(dirname $0)

killall distributed-store || true
sleep 0.1

go install -v

distributed-store -db-location=database/chisinau -http-addr=127.0.0.0:8080 -config-file=sharding.toml -shard=Chisinau -env=config/env/.env0 &

distributed-store -db-location=database/balti -http-addr=127.0.0.1:8080 -config-file=sharding.toml -shard=Balti -env=config/env/.env1 &

distributed-store -db-location=database/orhei -http-addr=127.0.0.2:8080 -config-file=sharding.toml -shard=Orhei -env=config/env/.env2  &

distributed-store -db-location=database/cahul -http-addr=127.0.0.3:8080 -config-file=sharding.toml -shard=Cahul -env=config/env/.env3 &

wait