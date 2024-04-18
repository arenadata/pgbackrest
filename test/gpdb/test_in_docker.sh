#!/usr/bin/env bash
set -eo pipefail

GPDB_PARENT=$(dirname "$GPDB_ROOT")
if [ ! -d "$GPDB_ROOT" ]; then
    echo "Directory with GPDB srcs $GPDB_ROOT does not exist. Exiting."
    exit 1
fi

function install_pgbackrest() {
    pushd $GPDB_PARENT/pgbackrest/src
    su gpadmin -c "
    ./configure &&
    make -j`nproc` -s &&
    make install DESTDIR=$1 bindir=$2 &&
    make clean"
    popd
}

function run_tests() {
    SUCCESS_COUNT=0
    FAILURE_COUNT=0
    tests_dir=$1
    for test_script in "$tests_dir"/*.sh; do
        echo "Running test: $(basename "$test_script")"
        if sudo -u gpadmin bash -c "$test_script $2 $3 $4 $GPHOME"; then
             SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            echo "Test failed $(basename "$test_script")"
             FAILURE_COUNT=$((FAILURE_COUNT + 1))
        fi
	done

    echo "======================="
    echo "Tests run: $((SUCCESS_COUNT + FAILURE_COUNT))"
    echo "Passed: $SUCCESS_COUNT"
    echo "Failed: $FAILURE_COUNT"
}

# we are doing copy here because the /tmp/pg_backrest/ dir is a readonly volume,
# but gpadmin has to work with his ownership on the repo. 
cp -r /tmp/pg_backrest/ $GPDB_PARENT/pgbackrest/

# configure GPDB
source $GPDB_ROOT/concourse/scripts/common.bash
$GPDB_ROOT/concourse/scripts/setup_gpadmin_user.bash "$TEST_OS"
install_and_configure_gpdb

# configure pgbackrest
install_pgbackrest $GPDB_PARENT/test_pgbackrest
run_tests $GPDB_PARENT/pgbackrest/test/gpdb/scripts $GPDB_ROOT \
$GPDB_PARENT/test_pgbackrest $GPDB_PARENT/test_pgbackrest
