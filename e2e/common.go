package e2e

import "github.com/Haylen-Z/godis/pkg"

var client pkg.Client

func setupClient() {
	client = pkg.NewClient("localhost:6379")
}

func teardownClient() {
	err := client.Close()
	if err != nil {
		panic(err)
	}
}
