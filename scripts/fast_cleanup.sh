#!/usr/bin/env bash
set -e
export PGPASSWORD=example
docker-compose restart
./scripts/wait_for_postgres.sh 5433
dropdb -h 127.0.0.1 -p 5433 -U postgres postgres
createdb -h 127.0.0.1 -p 5433 -U postgres postgres