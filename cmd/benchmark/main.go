package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Haylen-Z/godis/pkg"
)


func main() {
	var workerNum int
	var loop int
	flag.IntVar(&workerNum, "worker", 10, "worker number")
	flag.IntVar(&loop, "loop", 100, "loop number")
	flag.Parse()

	cli := pkg.NewClient("127.0.0.1")
	defer cli.Close()

	wg := sync.WaitGroup{}
	wg.Add(workerNum)
	startTime := time.Now()
	for i := 0; i < workerNum; i++ {
		go func () {
			exec(cli, loop)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("total time: %d ms\n", time.Since(startTime).Milliseconds())
}

func exec(cli pkg.Client, loop int) {
	for i := 0; i < loop; i++ {
		key := "k" + strconv.Itoa(i)
		v := "value" + strconv.Itoa(i)
		cli.Set(context.TODO(), key, []byte(v), pkg.NXArg, pkg.EXArg(10))
		cli.Get(context.TODO(), key)
	}
}
