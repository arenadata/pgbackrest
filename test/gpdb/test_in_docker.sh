#!/usr/bin/env bash
set -eo pipefail

function install_pgbackrest() {
    pushd /home/gpadmin/pgbackrest/src
    ./configure --enable-test &&
    make -j`nproc` -s &&
    make install &&
    make clean
    popd
}

function stop_and_clear_gpdb() {
    source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
    DATADIR="${MASTER_DATA_DIRECTORY%*/*/*}"
    # Attempt to stop the Greenplum Database cluster gracefully
    echo "Attempting to stop GPDB cluster gracefully..."
    su - gpadmin -c "
        export MASTER_DATA_DIRECTORY=$MASTER_DATA_DIRECTORY
        source /usr/local/greenplum-db-devel/greenplum_path.sh &&
        gpstop -af "

    # In case gpstop didn't work, force stop all postgres processes
    # This approach is a catch-all attempt in case the cluster is in a
    # very broken state
    if [ $? -ne 0 ]; then
        echo "Graceful stop failed. Attempting to force stop any \
        remaining postgres processes..."
        pkill -9 postgres
    fi
    sleep 2 # Giving it a moment before proceeding to the next steps

    # Now that all processes are ensured to be stopped,
    # clear the data directories.
    echo "Clearing GPDB data directories..."
    for dir in $DATADIR; do
        if [ -d "$dir" ]; then
            rm -rf "$dir"/* || true
        fi
    done
}



function run_tests() {
    SUCCESS_COUNT=0
    FAILURE_COUNT=0
    tests_dir=$1
    for test_script in "$tests_dir"/*.sh; do
        TEST_NAME=$(basename "${test_script%.sh}")
        echo "Running test: $TEST_NAME"
        if sudo -u gpadmin bash -c "bash $test_script"; then
             SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            echo "Test failed $TEST_NAME"
            FAILURE_COUNT=$((FAILURE_COUNT + 1))
        fi
        rm -r /home/gpadmin/test_pgbackrest/$TEST_NAME 2>/dev/null || true
        chmod o+r /home/gpadmin/test_pgbackrest/logs/$TEST_NAME/*
        stop_and_clear_gpdb
	done

    echo "======================="
    echo "Tests run: $((SUCCESS_COUNT + FAILURE_COUNT))"
    echo "Passed: $SUCCESS_COUNT"
    echo "Failed: $FAILURE_COUNT"

    if [ $FAILURE_COUNT -gt 0 ]; then
        exit 1
    fi
}

# configure GPDB
source gpdb_src/concourse/scripts/common.bash
gpdb_src/concourse/scripts/setup_gpadmin_user.bash
install_and_configure_gpdb

# configure pgbackrest
install_pgbackrest
run_tests /home/gpadmin/pgbackrest/test/gpdb/scripts
