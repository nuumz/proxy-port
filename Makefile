BINARY := proxy-port

.PHONY: build test vet fmt load clean

build:
	CGO_ENABLED=0 go build -ldflags "-s -w" -o $(BINARY) .

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

# Concurrent throughput benchmark through the forwarder (see bench_test.go).
load:
	go test -run '^$$' -bench BenchmarkForwardTCP -benchmem -benchtime 5s ./internal/forward/

clean:
	rm -f $(BINARY)
