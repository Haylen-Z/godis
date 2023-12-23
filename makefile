.PONY: test e2e

test: e2e_test pkg_test

benchmark:
	go run ./cmd/benchmark/main.go --worker 400 --loop 10000

e2e_test:
	go test ./e2e/...

pkg_test:
	go test ./pkg/...
