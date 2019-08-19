#!/usr/bin/env bash

if ! [ -x "$(command -v psql)" ];then
  echo "Install PostgreSQL cmdline client (sudo apt install postgresql-client)"
  exit 1
fi

docker-compose -f tests/docker-compose.yml up -d

./scripts/wait_for_postgres.sh 5434
pytest --disable-warnings tests

docker-compose -f tests/docker-compose.yml down