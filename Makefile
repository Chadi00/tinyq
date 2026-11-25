.PHONY: run build bench help

run:
	go run ./cmd/tinyq

build:
	go build -trimpath -ldflags="-s -w" -o bin/tinyq ./cmd/tinyq

bench: build
	go run ./tools/bench $(BENCH_ARGS)

help:
	@printf '%s\n' 'make run    Start tinyq' 'make build  Build the release binary in bin/' 'make bench  Benchmark the release binary with a temporary WAL'
