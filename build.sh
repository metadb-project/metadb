#!/bin/sh
set -e

fast='false'
json=''
runtest='false'
runalltest='false'
verbose='false'
experiment='false'

usage() {
    echo ''
    echo 'Usage:  build.sh [<flags>]'
    echo ''
    echo 'Flags:'
    echo '-f                            - "Fast" build (do not remove executables)'
    echo '-h                            - Help'
    echo '-t                            - Run tests'
    echo '-T                            - Run tests and all static analyses, requires:'
    echo '                                     honnef.co/go/tools/cmd/staticcheck@latest'
    echo '                                     github.com/kisielk/errcheck'
    echo '                                     gitlab.com/opennota/check/cmd/aligncheck'
    echo '                                     gitlab.com/opennota/check/cmd/structcheck'
    echo '                                     gitlab.com/opennota/check/cmd/varcheck'
    echo '                                     github.com/gordonklaus/ineffassign'
    echo '                                     github.com/remyoudompheng/go-misc/deadcode'
    echo '-v                            - Enable verbose output'
    echo '-X                            - Include experimental code'
}

while getopts 'JTcfhtvX' flag; do
    case "${flag}" in
        T) runalltest='true' ;;
        c) ;;
        f) fast='true' ;;
        J) echo "build.sh: -J option is deprecated" 1>&2 ;;
        h) usage
            exit 1 ;;
        t) runtest='true' ;;
        v) verbose='true' ;;
        X) experiment='true' ;;
        *) usage
            exit 1 ;;
    esac
done

shift $(($OPTIND - 1))
for arg; do
    if [ $arg = 'help' ]
    then
        usage
        exit 1
    fi
    echo "build.sh: unknown argument: $arg" 1>&2
    exit 1
done

if $verbose; then
    v='-v'
fi

if $experiment; then
    json='-X main.rewriteJSON=1'
    echo "build.sh: experimental code will be included" 1>&2
fi

bindir=bin

if ! $fast; then
    echo 'build.sh: removing executables' 1>&2
    rm -f ./$bindir/metadb ./$bindir/mdb
fi

echo 'build.sh: compiling Metadb' 1>&2

version=`git describe --tags --always`

mkdir -p $bindir

go build -o $bindir $v -ldflags "-X main.metadbVersion=$version $json" ./cmd/metadb
go build -o $bindir $v -ldflags "-X main.metadbVersion=$version" ./cmd/mdb

if $runalltest; then
    echo 'build.sh: running all static analyses' 1>&2
    echo 'vet' 1>&2
    go vet $v ./cmd/metadb 1>&2
    go vet $v ./cmd/mdb 1>&2
    echo 'staticcheck' 1>&2
    staticcheck ./cmd/metadb 1>&2
    staticcheck ./cmd/mdb 1>&2
    echo 'errcheck' 1>&2
    errcheck -exclude .errcheck ./cmd/metadb 1>&2
    errcheck -exclude .errcheck ./cmd/mdb 1>&2
    echo 'aligncheck' 1>&2
    aligncheck ./cmd/metadb 1>&2
    aligncheck ./cmd/mdb 1>&2
    echo 'structcheck' 1>&2
    structcheck ./cmd/metadb 1>&2
    structcheck ./cmd/mdb 1>&2
    echo 'varcheck' 1>&2
    varcheck ./cmd/metadb 1>&2
    varcheck ./cmd/mdb 1>&2
    echo 'ineffassign' 1>&2
    ineffassign ./cmd/metadb 1>&2
    ineffassign ./cmd/mdb 1>&2
    echo 'deadcode' 1>&2
    deadcode -test ./cmd/metadb 1>&2
    deadcode -test ./cmd/mdb 1>&2
fi

if $runtest || $runalltest; then
    echo 'build.sh: running tests' 1>&2
    go test $v -vet=off -count=1 ./cmd/metadb/command 1>&2
    go test $v -vet=off -count=1 ./cmd/metadb/sqlx 1>&2
    go test $v -vet=off -count=1 ./cmd/metadb/util 1>&2
fi

echo 'build.sh: compiled to executables in bin' 1>&2

