package e2e

import "github.com/Haylen-Z/godis"

var client godis.Client

func setupClient() {
	var err error
	client, err = godis.NewClient(&godis.ClientConfig{Address: "127.0.0.1:6379"})
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
