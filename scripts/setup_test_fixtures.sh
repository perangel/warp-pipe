#!/usr/bin/env bash

script="$0"
basename="$(dirname $script)"
sqldir="$basename/sql"

DB_HOST=${DB_HOST:-127.0.0.1}
DB_PORT=${DB_PORT:-6432}
DB_USER=${DB_USER:-test}
DB_PASS=${DB_PASS:-test}
DB_NAME=${DB_NAME:-test}

docker-compose -f $basename/../docker-compose.test.yml up -d

until nc -w 1 -z $DB_HOST $DB_PORT; do
    echo "Waiting for db... sleeping"
    sleep 2
done

docker-compose -f $basename/../docker-compose.test.yml \
    exec -T -e PGPASSWORD=$DB_PASS test_db \
    psql \
    -U $DB_USER \
    -d $DB_NAME \
    -c "$(cat $sqldir/create_fixture_schema.sql $sqldir/create_fixture_data.sql)"
