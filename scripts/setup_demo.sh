#!/usr/bin/env bash

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
    echo "Waiting for db... sleeping"
    sleep 2
done

docker-compose -f $basename/../docker-compose.demo.yml \
    exec -T -e PGPASSWORD=$DB_PASS source_db \
    psql \
    -U $DB_USER \
    -d $DB_NAME \
    -c "$(cat $sqldir/create_fixture_schema.sql $sqldir/create_fixture_data.sql)"

warp-pipe setup-db -H $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -P $DB_PASS
