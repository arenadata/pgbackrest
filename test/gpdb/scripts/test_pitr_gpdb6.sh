#!/usr/bin/env bash
set -exo pipefail

PGBACKREST_TEST_DIR=/home/gpadmin/test_pgbackrest
PGBACKREST_BIN=/usr/local/bin
GPHOME=/usr/local/greenplum-db-devel

# Starting up demo cluster
source $GPHOME/greenplum_path.sh
pushd gpdb_src/gpAux/gpdemo
make create-demo-cluster WITH_MIRRORS=true
source gpdemo-env.sh
popd

# Creating backup and log directories for pgbackrest
TEST_NAME=$(basename "${0%.sh}")
mkdir -p "$PGBACKREST_TEST_DIR/logs/$TEST_NAME"
mkdir -p "$PGBACKREST_TEST_DIR/$TEST_NAME"
DATADIR="${MASTER_DATA_DIRECTORY%*/*/*}"
MASTER=${DATADIR}/qddir/demoDataDir-1
PRIMARY1=${DATADIR}/dbfast1/demoDataDir0
PRIMARY2=${DATADIR}/dbfast2/demoDataDir1
PRIMARY3=${DATADIR}/dbfast3/demoDataDir2
MIRROR1=${DATADIR}/dbfast_mirror1/demoDataDir0
MIRROR2=${DATADIR}/dbfast_mirror2/demoDataDir1
MIRROR3=${DATADIR}/dbfast_mirror3/demoDataDir2

# Filling the pgbackrest.conf configuration file
cat <<EOF > $PGBACKREST_TEST_DIR/pgbackrest.conf
[seg-1]
pg1-path=$MASTER
pg1-port=6000

[seg0]
pg1-path=$PRIMARY1
pg1-port=6002

[seg1]
pg1-path=$PRIMARY2
pg1-port=6003

[seg2]
pg1-path=$PRIMARY3
pg1-port=6004

[global]
repo1-path=$PGBACKREST_TEST_DIR/$TEST_NAME
log-path=$PGBACKREST_TEST_DIR/logs/$TEST_NAME
start-fast=y
fork=GPDB
EOF

# Initializing pgbackrest for GPDB
for i in -1 0 1 2
do 
    PGOPTIONS="-c gp_session_role=utility" $PGBACKREST_BIN/pgbackrest \
    --config $PGBACKREST_TEST_DIR/pgbackrest.conf --stanza=seg$i stanza-create
done

# Configuring WAL archiving command
gpconfig -c archive_mode -v on

gpconfig -c archive_command -v "'PGOPTIONS=\"-c gp_session_role=utility\" \
$PGBACKREST_BIN/pgbackrest --config $PGBACKREST_TEST_DIR/pgbackrest.conf \
--stanza=seg%c archive-push %p'" --skipvalidation

gpstop -ar

# pgbackrest health check
for i in -1 0 1 2
do 
    PGOPTIONS="-c gp_session_role=utility" $PGBACKREST_BIN/pgbackrest \
    --config $PGBACKREST_TEST_DIR/pgbackrest.conf --stanza=seg$i check
done

# The test scenario starts here
# Creating initial dataset
createdb gpdb_pitr_database
export PGDATABASE=gpdb_pitr_database
psql -c "CREATE TABLE t1 (id int, text varchar(255)) DISTRIBUTED BY (id);"

psql -c "INSERT INTO t1 SELECT i, 'text'||i FROM generate_series(1,30) i;"

# Creating full backup on master and seg0
for i in -1 0
do 
    PGOPTIONS="-c gp_session_role=utility" $PGBACKREST_BIN/pgbackrest \
    --config $PGBACKREST_TEST_DIR/pgbackrest.conf --stanza=seg$i \
    --type=full backup
done

# Checking the presence of first backup
function check_backup(){
    segment_backup_dir=$PGBACKREST_TEST_DIR/$TEST_NAME/backup/seg$1
    current_date=$(date +%Y%m%d)
    [[ -n $(find "$segment_backup_dir" -maxdepth 1 -type d -name "${current_date}-??????F" -not -empty) ]]
}
for i in -1 0
do 
   check_backup $i
done

# Creating additional table
psql -c "CREATE TABLE t2 (id int, text varchar(255)) DISTRIBUTED BY (id);"

psql -c "INSERT INTO t2 SELECT i, 'text'||i FROM generate_series(1,30) i;"

psql -c "SELECT * FROM t2 ORDER BY id;" \
-o "$PGBACKREST_TEST_DIR/$TEST_NAME/t2_rows_original.out"

# Filling the first table with more data at seg0 after the first backup
psql -c "INSERT INTO t1 SELECT 3, 'text'||i FROM generate_series(1,10) i;"

psql -c "SELECT * FROM t1 ORDER BY id;" \
-o "$PGBACKREST_TEST_DIR/$TEST_NAME/t1_rows_original.out"

# Creating full backup on seg1 and seg2
for i in 1 2
do 
    PGOPTIONS="-c gp_session_role=utility" $PGBACKREST_BIN/pgbackrest \
    --config $PGBACKREST_TEST_DIR/pgbackrest.conf --stanza=seg$i \
    --type=full backup
done

# Checking the presence of second backup
for i in 1 2
do 
   check_backup $i
done

# Creating a distributed restore point..."
psql -c "create extension gp_pitr;"
psql -c "select gp_create_restore_point('test_pitr');"
psql -c "select gp_switch_wal();"

# Simulating disaster
psql -c "drop table t1;"
psql -c "truncate table t2;"

gpstop -a
rm -rf "${MASTER:?}/"* "${PRIMARY1:?}/"* "${PRIMARY2:?}/"* "${PRIMARY3:?}/"*
rm -rf "${MIRROR1:?}/"* "${MIRROR2:?}/"* "${MIRROR3:?}/"* "$DATADIR/standby/"*

# Restoring cluster
for i in -1 0 1 2
do 
    PGOPTIONS="-c gp_session_role=utility" $PGBACKREST_BIN/pgbackrest --config \
    $PGBACKREST_TEST_DIR/pgbackrest.conf --stanza=seg$i --type=name \
    --target=test_pitr restore
done

# Configuring mirrors after primary restore
gpstart -am
gpinitstandby -ar

PGOPTIONS="-c gp_session_role=utility" psql << EOF
SET allow_system_table_mods to true;
UPDATE gp_segment_configuration
SET status = CASE WHEN role='m' THEN 'd' ELSE status END, mode = 'n'
WHERE content >= 0;
EOF

gpstop -ar
gprecoverseg -aF
gpinitstandby -as "$HOSTNAME" -S "$DATADIR/standby/" -P 6001

# Checking data integrity
psql -c "SELECT * FROM t1 ORDER BY id;" \
-o "$PGBACKREST_TEST_DIR/$TEST_NAME/t1_rows_restored.out"

psql -c "SELECT * FROM t2 ORDER BY id;" \
-o "$PGBACKREST_TEST_DIR/$TEST_NAME/t2_rows_restored.out"

diff "$PGBACKREST_TEST_DIR/$TEST_NAME/t1_rows_original.out" "$PGBACKREST_TEST_DIR/$TEST_NAME/t1_rows_restored.out"

diff "$PGBACKREST_TEST_DIR/$TEST_NAME/t2_rows_original.out" "$PGBACKREST_TEST_DIR/$TEST_NAME/t2_rows_restored.out"
