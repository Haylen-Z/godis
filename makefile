.PONY: test integration-test unit-test

test: unit-test integration-test

benchmark:
	go run ./cmd/benchmark/main.go --worker 100 --loop 1000

mockgen:
	mockgen -destination ./mocks.go  -self_package github.com/Haylen-Z/godis  -package godis  . Protocol,Connection,ConnectionPool
	mockgen -destination ./net_mocks.go  -package godis  net Conn

integration-test:
	go test -race ./integration/...

unit-test:
	go test  -race .
