.PONY: test e2e

test: e2e_test pkg_test

e2e_test:
	go test ./e2e/...

pkg_test:
	go test ./pkg/...
