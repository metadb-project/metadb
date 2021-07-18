#!/bin/sh
set -e

clean_f='false'
lint_f='false'
test_f='false'
verbose_f='false'

usage() {
	echo ''
	echo 'Usage:  build.sh [<flags>]'
	echo ''
	echo 'Flags:'
	echo '-c                            - Clean (remove executables) before building'
	echo '-h                            - Help'
	echo '-l                            - Run all linters (requires golangci-lint)'
	echo '-t                            - Run tests'
	echo '-v                            - Enable verbose output'
}

while getopts 'chltv' flag; do
    case "${flag}" in
        c) clean_f='true' ;;
        h) usage
            exit 1 ;;
        l) lint_f='true' ;;
        t) test_f='true' ;;
        v) verbose_f='true' ;;
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

if $verbose_f; then
    v='-v'
fi

bindir=bin

if $clean_f; then
    echo 'build.sh: removing executables' 1>&2
    rm -f ./$bindir/metadb ./$bindir/mdb
fi

if $lint_f; then
    echo 'build.sh: running all linters' 1>&2
    golangci-lint run $v 1>&2
else
    echo 'build.sh: running linter' 1>&2
    pkg=metadb
    go vet $v ./cmd/$pkg 1>&2
    pkg=mdb
    go vet $v ./cmd/$pkg 1>&2
fi

echo 'build.sh: compiling Metadb' 1>&2

version=`git describe --tags --always`

mkdir -p $bindir

pkg=metadb
go build -o $bindir $v -ldflags "-X main.metadbVersion=$version" ./cmd/$pkg

pkg=mdb
go build -o $bindir $v -ldflags "-X main.metadbVersion=$version" ./cmd/$pkg

if $test_f; then
    echo 'build.sh: running tests' 1>&2
    go test $v -count=1 ./cmd/metadb/util 1>&2
fi

echo 'build.sh: compiled to executables in bin' 1>&2

