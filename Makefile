GO    ?= go
SEEDS ?= 1000

.PHONY: all build vet test test-short race chaos bugzoo bench tla

all: vet test build

build:
	$(GO) build -o bin/ ./cmd/...

vet:
	$(GO) vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

test:
	$(GO) test ./...

test-short:
	$(GO) test -short ./...

# The race detector matters for the real, multi-goroutine server. The
# simulated code runs on one goroutine by design.
race:
	$(GO) test -race ./server ./transport ./client

chaos: build
	./bin/raftlab-chaos -seeds $(SEEDS)

bugzoo: build
	./bin/raftlab-chaos -bugzoo -seeds 3000 -out results/bugzoo.json

bench: build
	./scripts/bench.sh

tla:
	./scripts/tlc.sh
