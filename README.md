# godis

A Redis client for Go.

## Getting started

```golang
import (
    "github.com/Haylen-Z/godis/pkg"
    "fmt"
)

client, err = pkg.NewClient(&pkg.ClientConfig{Address: "127.0.0.1:6379"})
if err != nil {
	panic(err)
}
defer client.Close()
ctx := context.Background()

key := "key1"
val : "hh1"
ok, err := client.Set(ctx, key, []byte(val), pkg.NXArg)
if err != nil {
	panic(err)
}
if !ok {
    fmt.Println("Key aleady exists")
}

r, err := client.Get(ctx, key)
if err != nil {
    panic(err)
}
if r == nil {
    fmt.Println("Key not exists")
}
fmt.Println(strint(*r))

```

### Pipeline
```golang
    pipe := client.Pipeline()
    pipe.Set(key, []byte(val),  pkg.NXArg)
    pipe.Get(key)
    rs, err := pipe.Exec(ct)
    if err != nil {
        panic(err)
    }
    if !rs[0].(bool) {
        fmt.Println("Key aleady exists")
    }
    v := res[1].(*[]byte)
    fmt.Println(string(*v))
```
