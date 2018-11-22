#!/usr/bin/env bash

/usr/bin/docker-compose -f tests/docker-compose.yml up -d

pytest tests

/usr/bin/docker-compose -f tests/docker-compose.yml down