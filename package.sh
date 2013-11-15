./build.sh darwin/amd64 true   &&
./build.sh linux/amd64 true    &&
./build.sh windows/amd64 true  &&
gzip -9 bin/*                  &&
mv bin/* builds/
