#!/usr/bin/env bash

script="$0"
basename="$(dirname $script)"

DB_HOST=127.0.0.1
DB_PORT=5432
DB_USER=demo
DB_PASS=secret
DB_NAME=demo

docker-compose -f $basename/../docker-compose.demo.yml \
    exec -T -e PGPASSWORD=secret source_db \
    psql \
    -h 127.0.0.1 \
    -U demo \
    -d demo \
    -c ""
