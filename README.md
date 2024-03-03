# godis

A Redis client for Go.

## Getting started

```golang
import (
    "github.com/Haylen-Z/godis"
)

client, err := godis.NewClient(&godis.ClientConfig{Address: "127.0.0.1:6379"})
defer client.Close()
ctx := context.Background()

key := "key1"
val : "hh1"
ok, err := client.Set(ctx, key, val, godis.NXArg)
r, err := client.Get(ctx, key)
```

### Pipeline
```golang
    pipe := client.Pipeline()
    pipe.Set(key, []byte(val),  godis.NXArg)
    pipe.Get(key)
    rs, err := pipe.Exec(ct)
    // Set return
    setOk := res[0].(bool)
    // Get return
    val := res[1].(*string)
```
