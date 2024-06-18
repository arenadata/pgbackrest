#!/usr/bin/env bash
set -exo pipefail

DOCKERIMAGE="${1:-gpdb6_regress:latest}"
SCRIPT_PATH="$(dirname "$(realpath -s "$0")")"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_PATH")")"

docker build -t pgbackrest:test -f "$SCRIPT_PATH/Dockerfile" \
"$PROJECT_ROOT" --build-arg GPDB_IMAGE="$DOCKERIMAGE"

docker run --rm  \
--sysctl kernel.sem="500 1024000 200 4096" \
-v "$(pwd)/logs":/home/gpadmin/test_pgbackrest/logs pgbackrest:test \
bash "/home/gpadmin/pgbackrest/test/gpdb/test_in_docker.sh"
