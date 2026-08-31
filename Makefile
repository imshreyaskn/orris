.PHONY: all build start stop demo test check lint fmt clean visualizer

all: check build

build:
	go build -o bin/orrisd ./cmd/orrisd
	go build -o bin/orrisctl ./cmd/orrisctl

test:
	go test -v -count=1 ./...

lint:
	go vet ./...

fmt:
	gofmt -s -w .

check: fmt lint test

start:
	go run ./cmd/orrisctl start

stop:
	go run ./cmd/orrisctl stop

demo:
	go run ./cmd/orrisctl demo

kill-leader:
	go run ./cmd/orrisctl kill-leader

visualizer:
	go run ./cmd/orrisctl

clean:
	go run ./cmd/orrisctl stop
	rm -rf data/*/*.log data/*/*.wal bin/* *.pid *.log
