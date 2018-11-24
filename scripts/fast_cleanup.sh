#!/usr/bin/env bash
dropdb -h 127.0.0.1 -p 5433 -U postgres postgres
createdb -h 127.0.0.1 -p 5433 -U postgres postgres