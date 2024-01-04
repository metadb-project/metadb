#!/bin/sh
set -e

fast='false'
json=''
runtests='false'
runalltests='false'
verbose='false'
experiment='false'
tagsdynamic='false'
checkpointsegmentsize=''
maxpollinterval=''

usage() {
    echo 'Usage:  build.sh [<flags>]'
    echo ''
    echo 'Builds the "metadb" executable in the bin directory'
    echo ''
    echo 'Flags:'
    echo '-f  Do not remove executable before compiling'
    echo '-h  Help'
    echo '-t  Run tests'
    echo '-T  Run tests and other checks; requires'
    echo '    go install golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow@latest'
    # echo '    go install golang.org/x/tools/cmd/deadcode@latest'
    echo '    go install github.com/kisielk/errcheck@latest'
    echo '-v  Enable verbose output'
    # echo '-D  Enable "-tags dynamic" compiler option'
    echo '-C  Reduce checkpoint segment size (experimental)'
    echo '-P  Increase maximum poll interval (experimental)'
    # echo '-X  Include experimental code'
}

while getopts 'cfhJtvTXDCP' flag; do
    case "${flag}" in
        t) runtests='true' ;;
        T) runalltests='true' ;;
        c) ;;
        f) fast='true' ;;
        J) echo "build.sh: -J option is deprecated" 1>&2 ;;
        h) usage
            exit 1 ;;
        v) verbose='true' ;;
        X) experiment='true' ;;
        D) tagsdynamic='true' ;;
        C) checkpointsegmentsize='-X github.com/metadb-project/metadb/cmd/metadb/util.XCheckpointSegmentSize=10000' ;;
        P) maxpollinterval='-X github.com/metadb-project/metadb/cmd/metadb/util.XMaxPollInterval=3600000' ;;
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
    echo "The \"include experimental code\" option (-X) has been selected."
    read -p "This may prevent later upgrades.  Are you sure? " yn
    case $yn in
        [Yy] ) break ;;
        [Yy][Ee][Ss] ) break ;;
        * ) echo "Exiting" 1>&2
            exit 1 ;;
    esac
    # json='-X main.rewriteJSON=1'
    echo "build.sh: experimental code will be included" 1>&2
fi

bindir=bin

if ! $fast; then
    rm -f ./$bindir/metadb ./$bindir/mdb ./cmd/metadb/parser/gram.go ./cmd/metadb/parser/scan.go ./cmd/metadb/parser/y.output
fi

version=`git describe --tags --always`

# Check which operating system is running.
case "$(uname -s)" in
    Linux*)     tags='' ;;
    # Darwin*)    tags='-tags dynamic' ;;
    Darwin*)    tags='' ;;
    *)          tags='' ;;
esac

if $tagsdynamic; then
    tags='-tags dynamic'
fi

mkdir -p $bindir

go generate $v ./...

go build -o $bindir $v $tags -ldflags "-X github.com/metadb-project/metadb/cmd/metadb/util.MetadbVersion=$version $json $checkpointsegmentsize $maxpollinterval" ./cmd/metadb
# go build -o $bindir $v $tags -ldflags "-X main.metadbVersion=$version" ./cmd/mdb

if $runtests || $runalltests; then
    go test $v $tags -vet=off -count=1 ./cmd/metadb/command 1>&2
#    go test $v $tags -vet=off -count=1 ./cmd/metadb/dbx 1>&2
#    go test $v $tags -vet=off -count=1 ./cmd/metadb/parser 1>&2
#    go test $v $tags -vet=off -count=1 ./cmd/metadb/sqlx 1>&2
    go test $v $tags -vet=off -count=1 ./cmd/metadb/util 1>&2
fi

if $runalltests; then
    go vet $v $tags $(go list ./cmd/... | grep -v 'github.com/metadb-project/metadb/cmd/metadb/parser') 2>&1 | while read s; do echo "build.sh: $s" 1>&2; done
    go vet $v $tags -vettool=$GOPATH/bin/shadow ./cmd/... 2>&1 | while read s; do echo "build.sh: $s" 1>&2; done
    # deadcode -test ./cmd/... 2>&1 | while read s; do echo "build.sh: deadcode: $s" 1>&2; done
    if $verbose; then
        # Using -verbose outputs the function signature for .errcheck.
	verrcheck='-verbose'
    fi
    errcheck $verrcheck -exclude .errcheck ./cmd/... 2>&1 | while read s; do echo "build.sh: errcheck: $s" 1>&2; done
fi
