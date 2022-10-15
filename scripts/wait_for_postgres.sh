#!/usr/bin/env bash
host="127.0.0.1"
export PGPASSWORD=${2}

until psql -h ${host} -U "postgres" -p ${1} -c '\q' > /dev/null 2>&1; do
  echo "Postgres is unavailable - sleeping"
  sleep 1
done

echo "Postgres is up"
