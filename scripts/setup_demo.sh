#!/usr/bin/env bash

set -e

script="$0"
basename="$(dirname $script)"
sqldir="$basename/sql"

DB_HOST=${DB_HOST:-127.0.0.1}
DB_PORT=${DB_PORT:-5432}
DB_USER=${DB_USER:-demo}
DB_PASS=${DB_PASS:-demo}
DB_NAME=${DB_NAME:-demo}

docker-compose -f $basename/../docker-compose.demo.yml up -d

until nc -w 1 -z $DB_HOST $DB_PORT; do
    echo "Waiting for source db... sleeping"
    sleep 2
done
until nc -w 1 -z $DB_HOST 6432; do
    echo "Waiting for target db... sleeping"
    sleep 2
done

echo "db available"
sleep 2

echo "Setup Schema on Source"
docker-compose -f $basename/../docker-compose.demo.yml \
    exec -T -e PGPASSWORD=$DB_PASS source_db \
    psql \
    -h $DB_HOST \
    -p $DB_PORT \
    -U $DB_USER \
    -d $DB_NAME \
    -c "$(cat $sqldir/create_fixture_schema.sql)"

echo "Setup Schema on Target"
docker-compose -f $basename/../docker-compose.demo.yml \
    exec -T -e PGPASSWORD=$DB_PASS target_db \
    psql \
    -h $DB_HOST \
    -U $DB_USER \
    -d $DB_NAME \
    -c "$(cat $sqldir/create_fixture_schema.sql)"

warp-pipe setup-db -H $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -P $DB_PASS

echo "Load the websocket demo app: http://localhost:8080/"
read -p "Then press <Enter> to import SQL fixtures to see Warp Pipe Changesets get created..."

echo "Add data to Source"
docker-compose -f $basename/../docker-compose.demo.yml \
    exec -T -e PGPASSWORD=$DB_PASS source_db \
    psql \
    -h $DB_HOST \
    -U $DB_USER \
    -d $DB_NAME \
    -c "$(cat $sqldir/create_fixture_data.sql)"

echo "Run axon to sync all changes. CTRL-C to exit when done."

AXON_SOURCE_DB_HOST=localhost \
AXON_SOURCE_DB_NAME=demo \
AXON_SOURCE_DB_PASS=demo \
AXON_SOURCE_DB_PORT=5432 \
AXON_SOURCE_DB_USER=demo \
AXON_TARGET_DB_HOST=localhost \
AXON_TARGET_DB_NAME=demo \
AXON_TARGET_DB_PASS=demo \
AXON_TARGET_DB_PORT=6432 \
AXON_TARGET_DB_USER=demo \
go run cmd/axon/*.go
