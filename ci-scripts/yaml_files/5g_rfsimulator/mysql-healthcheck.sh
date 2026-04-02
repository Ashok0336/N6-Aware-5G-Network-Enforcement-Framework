#!/usr/bin/env bash
set -euo pipefail

# Simple MySQL liveness check
mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PASSWORD}" --silent
