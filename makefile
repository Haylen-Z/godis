.PONY: test e2e

test: e2e_test pkg_test

benchmark:
	go run ./cmd/benchmark/main.go --worker 100 --loop 1000

e2e_test:
	go test -race ./e2e/...

pkg_test:
	go test  -race ./pkg/...
