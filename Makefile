BIN     := tatami
PKG     := ./cmd/tatami
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X github.com/tamnd/tatami/cli.Version=$(VERSION) \
	-X github.com/tamnd/tatami/cli.Commit=$(COMMIT) \
	-X github.com/tamnd/tatami/cli.Date=$(DATE)

export CGO_ENABLED := 0

.PHONY: build install test test-short bench vet fmt tidy clean run

build:
	go build -ldflags "$(LDFLAGS)" -o bin/$(BIN) $(PKG)

install:
	go install -ldflags "$(LDFLAGS)" $(PKG)

# Full suite with the race detector on.
test:
	go test -race ./...

# Quick loop without the race detector.
test-short:
	go test -short ./...

bench:
	go test -bench=. -benchmem -run='^$$' ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

clean:
	rm -rf bin dist

run: build
	./bin/$(BIN)
