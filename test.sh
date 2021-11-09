#!/bin/sh
set -e

testid="_tmp_metadb_test_"
testdb=$testid
testdir=$testid
testdatadir=./$testdir/data
testlogfile=./$testdir/metadb.log

./build.sh -f

dropdb --if-exists --force $testdb
createdb -O metadbadmin $testdb
rm -rf $testlogfile $testdatadir
./bin/metadb init -D ./_tmp_metadb_test_/data

