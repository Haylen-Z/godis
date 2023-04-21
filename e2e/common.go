package e2e

import "github.com/Haylen-Z/godis/pkg"

var client pkg.Client

func setupClient() {
	var err error
	client, err = pkg.NewClient("localhost:6379")
	if err != nil {
		panic(err)
	}
}

func teardownClient() {
	err := client.Close()
	if err != nil {
		panic(err)
	}
}
