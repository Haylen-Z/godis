package main

import (
	"context"
	"flag"

	"github.com/Haylen-Z/godis"
)

func main() {
	var keyPath, certPath, caPath string
	flag.StringVar(&keyPath, "key", "", "key file")
	flag.StringVar(&certPath, "cert", "", "cert ")
	flag.StringVar(&caPath, "ca", "", "ca")
	flag.Parse()

	if keyPath == "" || certPath == "" || caPath == "" {
		panic("invalid parameters")
	}

	cli, err := godis.NewClient(&godis.ClientConfig{
		Address:       "localhost:6379",
		Tls:           true,
		TlsCertPath:   certPath,
		TlsCaCertPath: caPath,
		TlsKeyPath:    keyPath,
	})
	if err != nil {
		panic(err)
	}

	_, err = cli.Get(context.Background(), "k")
	if err != nil {
		panic(err)
	}
}
