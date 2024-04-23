#!/usr/bin/env bash
set -ex 

DOCKERIMAGE="$1"
DOCKER_GPDB_SRC_PATH="${2:-/home/gpadmin/gpdb_src}"
LOG_DIR="${3:-$(pwd)/logs}"
DOCKER_GPDB_SRC_PARENT=$(dirname "$DOCKER_GPDB_SRC_PATH")
SCRIPT_PATH=$(dirname $(realpath -s $0))
PROJECT_ROOT=$(dirname $(dirname "$SCRIPT_PATH"))

docker run --rm  \
-e GPDB_ROOT=$DOCKER_GPDB_SRC_PATH \
--sysctl kernel.sem="500 1024000 200 4096" \
-v $PROJECT_ROOT:/tmp/pg_backrest:ro \
-v $LOG_DIR:$DOCKER_GPDB_SRC_PARENT/test_pgbackrest/logs \
${DOCKERIMAGE} /bin/bash -c "bash /tmp/pg_backrest/test/gpdb/test_in_docker.sh"
