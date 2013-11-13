#!/bin/bash

TARGET=${1?:"Please specify a build target: darwin/386, darwin/amd64, freebsd/386, freebsd/amd64, linux/386, linux/amd64"}
RECOMPILE=${2}

export GOROOT=/tmp/go
export GOBIN=$GOROOT/bin
export GOPATH=$(pwd)
export GOOS=${TARGET%/*}
export GOARCH=${TARGET#*/}

date
echo "tt build script starting up"
echo

if [[ -z "$RECOMPILE" ]]; then
	(
		echo "Ensuring $GOROOT"   && mkdir -p $GOROOT && cd $GOROOT &&
		echo "Downloading source" && ([ -d .hg ] || hg clone https://code.google.com/p/go .) && hg pull && hg up default &&
		echo "Building stdlib"    && cd src && ./make.bash --no-clean 2>&1
	)
	if [[ $? -ne 0 ]]; then
		echo "Go build failed.  Exiting..." && exit 1
	fi
fi

# convert, grab deps, clear existing builds, and build
echo "go get..."   && $GOBIN/go get  &&
echo "go build..." && $GOBIN/go build -o "bin/tt-$GOOS-$GOARCH" tt.go

if [[ $? -ne 0 ]]; then
	echo "tt build failed.  Exiting..." && exit 1
fi

file bin/*

echo "Done!"
echo
