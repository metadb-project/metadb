#!/bin/sh
set -e

fast='false'
json=''
runalltest='false'
verbose='false'
quiet='false'
experiment='false'

usage() {
    echo ''
    echo 'Usage:  build.sh [<flags>]'
    echo ''
    echo 'Flags:'
    echo '-f  "Fast" build (do not remove executables or run tests)'
    echo '-h  Help'
    echo '-t  Run more checks; requires'
    echo '    go install golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow@latest'
    # echo '    go install honnef.co/go/tools/cmd/staticcheck@latest'
    echo '    go install github.com/kisielk/errcheck@latest'
    echo '    go install github.com/gordonklaus/ineffassign@latest'
    echo '    go install github.com/remyoudompheng/go-misc/deadcode@latest'
    echo '-v  Enable verbose output'
    # echo '-q  Enable quiet output'
    # echo '-X  Include experimental code'
}

while getopts 'cfhJtvqX' flag; do
    case "${flag}" in
        t) runalltest='true' ;;
        c) ;;
        f) fast='true' ;;
        J) echo "build.sh: -J option is deprecated" 1>&2 ;;
        h) usage
            exit 1 ;;
        v) verbose='true' ;;
        q) quiet='true' ;;
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
    # if ! $quiet; then
    #     echo 'build.sh: removing executables' 1>&2
    # fi
    rm -f ./$bindir/metadb ./$bindir/mdb ./cmd/metadb/parser/gram.go ./cmd/metadb/parser/scan.go ./cmd/metadb/parser/y.output
fi

# if ! $quiet; then
#     echo 'build.sh: compiling Metadb' 1>&2
# fi

version=`git describe --tags --always`

# Check which operating system is running.
case "$(uname -s)" in
    Linux*)     tags='' ;;
    Darwin*)    tags='-tags dynamic' ;;
    *)          tags='' ;;
esac

mkdir -p $bindir

go generate $v ./...

go build -o $bindir $v $tags -ldflags "-X github.com/metadb-project/metadb/cmd/metadb/util.MetadbVersion=$version $json" ./cmd/metadb
# go build -o $bindir $v $tags -ldflags "-X main.metadbVersion=$version" ./cmd/mdb

if $runalltest; then
    if ! $quiet; then
        echo 'build.sh: running tests' 1>&2
    fi
    go test $v $tags -vet=off -count=1 ./cmd/metadb/command 1>&2
#    go test $v $tags -vet=off -count=1 ./cmd/metadb/dbx 1>&2
#    go test $v $tags -vet=off -count=1 ./cmd/metadb/parser 1>&2
#    go test $v $tags -vet=off -count=1 ./cmd/metadb/sqlx 1>&2
    go test $v $tags -vet=off -count=1 ./cmd/metadb/util 1>&2
fi

if $runalltest; then
    if ! $quiet && ! $fast; then
        echo 'build.sh: running: vet' 1>&2
    fi
    go vet $v $tags $(go list ./... | grep -v 'github.com/metadb-project/metadb/cmd/metadb/parser') 1>&2
    if ! $quiet; then
        echo 'build.sh: running: vet shadow' 1>&2
    fi
    go vet $v $tags -vettool=$GOPATH/bin/shadow ./cmd/... 1>&2
    # echo 'build.sh: running: staticcheck' 1>&2
    # staticcheck ./cmd/... 1>&2
    if ! $quiet; then
        echo 'build.sh: running: errcheck' 1>&2
    fi
    # Add -verbose to get the function signature for .errcheck.
    errcheck -exclude .errcheck ./cmd/... 1>&2
    # echo 'build.sh: running: aligncheck' 1>&2
    # aligncheck ./cmd/metadb 1>&2
    # aligncheck ./cmd/mdb 1>&2
    # echo 'build.sh: running: structcheck' 1>&2
    # structcheck ./cmd/metadb 1>&2
    # structcheck ./cmd/mdb 1>&2
    # echo 'build.sh: running: varcheck' 1>&2
    # varcheck ./cmd/metadb 1>&2
    # varcheck ./cmd/mdb 1>&2
    if ! $quiet; then
        echo 'build.sh: running: ineffassign' 1>&2
    fi
    ineffassign ./cmd/... 1>&2
    if ! $quiet; then
        echo 'build.sh: running: deadcode' 1>&2
    fi
    deadcode -test ./cmd/metadb 1>&2
#    deadcode -test ./cmd/mdb 1>&2
fi

if ! $quiet; then
    echo 'build.sh: compiled to executables in bin' 1>&2
fi
