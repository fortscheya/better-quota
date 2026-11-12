#!/bin/sh
# Offline verification in an isolated Go container. Never targets the running CPA.
set -eu
cd "$(dirname "$0")"
version=$(sed -n 's/^const version = "\([^"]*\)"/\1/p' config.go)
test -n "$version"
stage=$(mktemp -d "${TMPDIR:-/tmp}/better-quota-build.XXXXXX")
container=""
cleanup() {
 if [ -n "$container" ]; then docker rm -f "$container" >/dev/null; fi
 rm -rf "$stage"
}
trap cleanup EXIT INT TERM
tar --exclude=.git --exclude=.project-continuity --exclude=dist -cf - . | tar -xf - -C "$stage"
container=$(docker create --network none --platform linux/arm64 -e BQ_VERSION="$version" -w /src golang:1.26-bookworm sh -ec '
 mkdir -p dist
 test -z "$(gofmt -l *.go)"
 go test -mod=vendor -race -coverprofile=dist/coverage.out -count=1 -timeout=120s ./...
 go vet -mod=vendor ./...
 go build -mod=vendor -buildmode=c-shared -trimpath -buildvcs=false -ldflags="-s -w" -o dist/better-quota-v${BQ_VERSION}.so .
 gcc -std=c11 -Wall -Wextra -Werror -o dist/abi-smoke tests/abi_smoke.c -ldl
 dist/abi-smoke ./dist/better-quota-v${BQ_VERSION}.so
 sha256sum dist/better-quota-v${BQ_VERSION}.so > dist/SHA256SUMS
')
docker cp "$stage/." "$container:/src"
docker start "$container" >/dev/null
result=$(docker wait "$container")
docker logs "$container"
[ "$result" = 0 ] || exit 1
mkdir -p dist
docker cp "$container:/src/dist/." dist/
