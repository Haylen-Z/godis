.PONY: test e2e

test: e2e_test pkg_test

benchmark:
	go run ./cmd/benchmark/main.go --worker 100 --loop 1000

mockgen:
	mockgen -destination ./pkg/protocol_mock.go -self_package github.com/Haylen-Z/godis/pkg -package pkg   --source ./pkg/protocol.go Protocol
	mockgen -destination ./pkg/connection_mock.go -self_package github.com/Haylen-Z/godis/pkg -package pkg  --source ./pkg/connection.go Connection
	mockgen -destination ./pkg/net_con_mock.go -package pkg  net Conn

e2e_test:
	go test -race ./e2e/...

pkg_test:
	go test  -race ./pkg/...
