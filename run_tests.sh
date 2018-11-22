#!/usr/bin/env bash

/usr/bin/docker-compose -f tests/docker-compose.yml up -d

./scripts/wait_for_postgres.sh 5434
pytest --disable-warnings tests

/usr/bin/docker-compose -f tests/docker-compose.yml down