package e2e

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/Haylen-Z/godis/pkg"
	"github.com/stretchr/testify/assert"
)

func TestStringGetAndSet(t *testing.T) {
	setupClient()
	defer teardownClient()

	res, err := client.Set(context.TODO(), "hello", []byte("world"))
	assert.Nil(t, err)
	assert.True(t, res)

	val, err := client.Get(context.TODO(), "hello")
	assert.Nil(t, err)
	assert.Equal(t, "world", string(*val))

	res, err = client.Set(context.TODO(), "hello", []byte("world2"), pkg.EXArg(100), pkg.NXArg)
	assert.Nil(t, err)
	assert.False(t, res)

	res, err = client.Set(context.TODO(), "hello", []byte("world2"), pkg.XXArg, pkg.EXArg(100))
	assert.Nil(t, err)
	assert.True(t, res)
}

func TestConcurrent(t *testing.T) {
	setupClient()
	defer teardownClient()

	wg := sync.WaitGroup{}
	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			key := "hello" + strconv.Itoa(idx)
			val := "world" + strconv.Itoa(idx)

			res, err := client.Set(context.TODO(), key, []byte(val))
			assert.Nil(t, err)
			assert.True(t, res)

			v, err := client.Get(context.TODO(), key)
			assert.Nil(t, err)
			assert.Equal(t, val, string(*v))
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestStringAppend(t *testing.T) {
	setupClient()
	defer teardownClient()

	k := "kk"
	ctx := context.TODO()

	_, err := client.Set(ctx, k, []byte("iii"))
	assert.Nil(t, err)

	res, err := client.Append(ctx, k, []byte("iii"))
	assert.Nil(t, err)
	assert.Equal(t, int64(6), res)

	res, err = client.Append(ctx, k, []byte("wwwww"))
	assert.Nil(t, err)
	assert.Equal(t, int64(11), res)

	val, err := client.Get(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, "iiiiiiwwwww", string(*val))

	res, err = client.Append(ctx, k, []byte{})
	assert.Nil(t, err)
	assert.Equal(t, int64(11), res)
	val, err = client.Get(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, "iiiiiiwwwww", string(*val))
}

func TestStringDecr(t *testing.T) {
	setupClient()
	defer teardownClient()

	k := "kk"
	ctx := context.TODO()

	_, err := client.Set(ctx, k, []byte("0"))
	assert.Nil(t, err)

	res, err := client.Decr(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), res)

	_, err = client.Set(ctx, k, []byte("100"))
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		res, err = client.Decr(ctx, k)
		assert.Nil(t, err)
		assert.Equal(t, int64(100-(i+1)), res)
	}
}

func TestStringDecrBy(t *testing.T) {
	setupClient()
	defer teardownClient()

	k := "kk"
	ctx := context.TODO()

	_, err := client.Set(ctx, k, []byte("0"))
	assert.Nil(t, err)

	res, err := client.DecrBy(ctx, k, 2)
	assert.Nil(t, err)
	assert.Equal(t, int64(-2), res)

	res, err = client.DecrBy(ctx, k, -3)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), res)
}

func TestStringGetAndDel(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k := "kkk34213"
	res, err := client.GetDel(ctx, k)

	assert.Nil(t, err)
	assert.Nil(t, res)

	_, err = client.Set(ctx, k, []byte("hello"))
	assert.Nil(t, err)

	res, err = client.GetDel(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, "hello", string(*res))

	res, err = client.Get(ctx, k)
	assert.Nil(t, err)
	assert.Nil(t, res)
}

func TestGetEX(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k := "kgetex"
	res, err := client.GetEX(ctx, k)
	assert.Nil(t, err)
	assert.Nil(t, res)

	_, err = client.Set(ctx, k, []byte("hello"))
	assert.Nil(t, err)

	res, err = client.GetEX(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, "hello", string(*res))

	_, err = client.GetEX(ctx, k, pkg.EXATArg(100))
	assert.Nil(t, err)

	_, err = client.GetEX(ctx, k, pkg.PXATArg(100))
	assert.Nil(t, err)
}

func TestMGet(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	_, err := client.Set(ctx, "k1", []byte("v1"))
	assert.Nil(t, err)
	_, err = client.Set(ctx, "k2", []byte("v2"))
	assert.Nil(t, err)

	res, err := client.MGet(ctx, "k1", "k2", "k3")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, "v1", string(*(res[0].(*[]byte))))
	assert.Equal(t, "v2", string(*(res[1].(*[]byte))))
	assert.Nil(t, res[2])
}
