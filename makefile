.PONY: test integration-test unit-test

test: unit-test integration-test

benchmark:
	go run ./cmd/benchmark/main.go --worker 100 --loop 1000

mockgen:
	mockgen -destination ./pkg/protocol_mock.go -self_package github.com/Haylen-Z/godis/pkg -package pkg   --source ./pkg/protocol.go Protocol
	mockgen -destination ./pkg/connection_mock.go -self_package github.com/Haylen-Z/godis/pkg -package pkg  --source ./pkg/connection.go Connection
	mockgen -destination ./pkg/net_con_mock.go -package pkg  net Conn

integration-test:
	go test -race ./integration/...

unit-test:
	go test  -race ./pkg/...
