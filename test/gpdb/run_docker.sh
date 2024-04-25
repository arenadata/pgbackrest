#!/usr/bin/env bash
set -exo pipefail

DOCKERIMAGE="$1"
LOG_DIR="$2"
SCRIPT_PATH=$(dirname $(realpath -s $0))
PROJECT_ROOT=$(dirname $(dirname "$SCRIPT_PATH"))

if [ -z "$(docker images -q pgbackrest:test 2> /dev/null)" ]; then
  docker build -t pgbackrest:test -f $SCRIPT_PATH/Dockerfile \
  $PROJECT_ROOT --build-arg GPDB_IMAGE=$DOCKERIMAGE
fi


if [ ! -d "$LOG_DIR" ]; then
	docker run --rm  \
    --sysctl kernel.sem="500 1024000 200 4096" \
    -v $LOG_DIR:/home/gpadmin/test_pgbackrest/logs pgbackrest:test \
    /bin/bash -c "bash /home/gpadmin/pgbackrest/test/gpdb/test_in_docker.sh"
else
    docker run --rm  \
    --sysctl kernel.sem="500 1024000 200 4096" pgbackrest:test \
    /bin/bash -c "bash /home/gpadmin/pgbackrest/test/gpdb/test_in_docker.sh"
fi
