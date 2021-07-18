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
    echo '-c                            - Remove executables before building'
    echo '-h                            - Help'
    echo '-l                            - Run linters; requires "go get"'
    echo '                                  honnef.co/go/tools/cmd/staticcheck@latest'
    echo '                                  github.com/kisielk/errcheck'
    echo '                                  gitlab.com/opennota/check/cmd/aligncheck'
    echo '                                  gitlab.com/opennota/check/cmd/structcheck'
    echo '                                  gitlab.com/opennota/check/cmd/varcheck'
    echo '                                  github.com/gordonklaus/ineffassign'
    echo '                                  github.com/remyoudompheng/go-misc/deadcode'
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
    echo 'build.sh: linter: vet' 1>&2
    go vet $v ./cmd/metadb 1>&2
    go vet $v ./cmd/mdb 1>&2
    echo 'build.sh: linter: staticcheck' 1>&2
    staticcheck ./cmd/metadb 1>&2
    staticcheck ./cmd/mdb 1>&2
    echo 'build.sh: linter: errcheck' 1>&2
    errcheck -exclude .errcheck ./cmd/metadb 1>&2
    errcheck -exclude .errcheck ./cmd/mdb 1>&2
    echo 'build.sh: linter: aligncheck' 1>&2
    aligncheck ./cmd/metadb 1>&2
    aligncheck ./cmd/mdb 1>&2
    echo 'build.sh: linter: structcheck' 1>&2
    structcheck ./cmd/metadb 1>&2
    structcheck ./cmd/mdb 1>&2
    echo 'build.sh: linter: varcheck' 1>&2
    varcheck ./cmd/metadb 1>&2
    varcheck ./cmd/mdb 1>&2
    echo 'build.sh: linter: ineffassign' 1>&2
    ineffassign ./cmd/metadb 1>&2
    ineffassign ./cmd/mdb 1>&2
    echo 'build.sh: linter: deadcode' 1>&2
    deadcode -test ./cmd/metadb 1>&2
    deadcode -test ./cmd/mdb 1>&2
fi

echo 'build.sh: compiling Metadb' 1>&2

version=`git describe --tags --always`

mkdir -p $bindir

go build -o $bindir $v -ldflags "-X main.metadbVersion=$version" ./cmd/metadb
go build -o $bindir $v -ldflags "-X main.metadbVersion=$version" ./cmd/mdb

if $test_f; then
    echo 'build.sh: running tests' 1>&2
    go test $v -count=1 ./cmd/metadb/util 1>&2
fi

echo 'build.sh: compiled to executables in bin' 1>&2

