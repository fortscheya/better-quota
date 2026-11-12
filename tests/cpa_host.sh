#!/bin/sh
# Optional real CPA loader/RPC-adapter contract test in a disposable container.
# Public source dependencies may be downloaded; no live CPA is contacted.
set -eu
: "${CPA_SOURCE:?Point CPA_SOURCE at the public CPA v8.0.57 source}"
cd "$(dirname "$0")/.."
version=$(sed -n 's/^const version = "\([^"]*\)"/\1/p' config.go)
test -f "dist/better-quota-v$version.so"
rg -q '^module github.com/router-for-me/CLIProxyAPI/v8$' "$CPA_SOURCE/go.mod"
container=$(docker create --platform linux/arm64 -w /cpa -e BQ_VERSION="$version" -e BQ_PLUGIN_PATH=/better-quota.so -e GOTOOLCHAIN=auto -v better-quota-gomod:/go/pkg/mod -v better-quota-gobuild:/root/.cache/go-build golang:1.26-bookworm go test -count=1 -timeout=120s ./internal/pluginhost -run '^TestBetterQuotaNativeContract$' -v)
trap 'docker rm -f "$container" >/dev/null' EXIT INT TERM
docker cp "$CPA_SOURCE/." "$container:/cpa"
docker cp tests/cpa_host_test.go.in "$container:/cpa/internal/pluginhost/better_quota_contract_test.go"
docker cp "dist/better-quota-v$version.so" "$container:/better-quota.so"
docker start "$container" >/dev/null
result=$(docker wait "$container")
docker logs "$container"
[ "$result" = 0 ]
