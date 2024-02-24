package e2e

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/Haylen-Z/godis/pkg"
	"github.com/stretchr/testify/assert"
)

func TestStringGetAndSet(t *testing.T) {
	setupClient()
	defer teardownClient()

	res, err := client.Set(context.TODO(), "hello", "world")
	assert.Nil(t, err)
	assert.True(t, res)

	val, err := client.Get(context.TODO(), "hello")
	assert.Nil(t, err)
	assert.Equal(t, "world", string(*val))

	res, err = client.Set(context.TODO(), "hello", "world2", pkg.EXArg(100), pkg.NXArg)
	assert.Nil(t, err)
	assert.False(t, res)

	res, err = client.Set(context.TODO(), "hello", "world2", pkg.XXArg, pkg.EXArg(100))
	assert.Nil(t, err)
	assert.True(t, res)

	_, err = client.Set(context.TODO(), "hello", "world2", pkg.MINMATCHLENArg(1))
	assert.NotNil(t, err)
	assert.True(t, errors.As(err, &pkg.Error{}))
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

			res, err := client.Set(context.TODO(), key, val)
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

	_, err := client.Set(ctx, k, "iii")
	assert.Nil(t, err)

	res, err := client.Append(ctx, k, "iii")
	assert.Nil(t, err)
	assert.Equal(t, int64(6), res)

	res, err = client.Append(ctx, k, "wwwww")
	assert.Nil(t, err)
	assert.Equal(t, int64(11), res)

	val, err := client.Get(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, "iiiiiiwwwww", string(*val))

	res, err = client.Append(ctx, k, "")
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

	_, err := client.Set(ctx, k, "0")
	assert.Nil(t, err)

	res, err := client.Decr(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), res)

	_, err = client.Set(ctx, k, "100")
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

	_, err := client.Set(ctx, k, "0")
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

	_, err = client.Set(ctx, k, "hello")
	assert.Nil(t, err)

	res, err = client.GetDel(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, "hello", *res)

	res1, err := client.Get(ctx, k)
	assert.Nil(t, err)
	assert.Nil(t, res1)
}

func TestGetEX(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k := "kgetex"
	res, err := client.GetEX(ctx, k)
	assert.Nil(t, err)
	assert.Nil(t, res)

	_, err = client.Set(ctx, k, "hello")
	assert.Nil(t, err)

	res, err = client.GetEX(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, "hello", *res)

	_, err = client.GetEX(ctx, k, pkg.EXATArg(100))
	assert.Nil(t, err)

	_, err = client.GetEX(ctx, k, pkg.PXATArg(100))
	assert.Nil(t, err)
}

func TestMGet(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	_, err := client.Set(ctx, "k1", "v1")
	assert.Nil(t, err)
	_, err = client.Set(ctx, "k2", "v2")
	assert.Nil(t, err)

	res, err := client.MGet(ctx, "k1", "k2", "k34322432g")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, "v1", *res[0])
	assert.Equal(t, "v2", *res[1])
	assert.Nil(t, res[2])
}

func TestLcs(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k1 := "key1"
	_, err := client.Set(ctx, k1, "ohmytext")
	assert.Nil(t, err)

	k2 := "key2"
	_, err = client.Set(ctx, k2, "mynewtext")
	assert.Nil(t, err)

	res, err := client.Lcs(ctx, k1, k2)
	assert.Nil(t, err)
	assert.Equal(t, "mytext", res)

	l, err := client.LcsLen(ctx, k1, k2)
	assert.Nil(t, err)
	assert.Equal(t, int64(6), l)

	idx, err := client.LcsIdx(ctx, k1, k2)
	assert.Nil(t, err)
	assert.Equal(t, int64(6), idx.Len)
	assert.Equal(t, 2, len(idx.Matches))
	m := idx.Matches[1]
	assert.Equal(t, 2, m.Pos1[0])
	assert.Equal(t, 3, m.Pos1[1])
	assert.Equal(t, 0, m.Pos2[0])
	assert.Equal(t, 1, m.Pos2[1])

	idx, err = client.LcsIdx(ctx, k1, k2, pkg.MINMATCHLENArg(4))
	assert.Nil(t, err)
	assert.Equal(t, int64(6), idx.Len)
	assert.Equal(t, 1, len(idx.Matches))
	m = idx.Matches[0]
	assert.Equal(t, 4, m.Pos1[0])
	assert.Equal(t, 7, m.Pos1[1])
	assert.Equal(t, 5, m.Pos2[0])
	assert.Equal(t, 8, m.Pos2[1])

	idx, err = client.LcsIdxWithMatchLen(ctx, k1, k2, pkg.MINMATCHLENArg(4))
	assert.Nil(t, err)
	assert.Equal(t, int64(6), idx.Len)
	assert.Equal(t, 1, len(idx.Matches))
	m = idx.Matches[0]
	assert.Equal(t, 4, m.Pos1[0])
	assert.Equal(t, 7, m.Pos1[1])
	assert.Equal(t, 5, m.Pos2[0])
	assert.Equal(t, 8, m.Pos2[1])
	assert.Equal(t, 4, m.Len)
}

func TestGetRange(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k := "kgetrange"
	_, err := client.Set(ctx, k, "hello")
	assert.Nil(t, err)

	res, err := client.GetRange(ctx, k, 0, 3)
	assert.Nil(t, err)
	assert.Equal(t, "hell", res)

	res, err = client.GetRange(ctx, k, 2, -1)
	assert.Nil(t, err)
	assert.Equal(t, "llo", res)
}

func TestGetSet(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k := "kgetset"
	_, err := client.Set(ctx, k, "hello")
	assert.Nil(t, err)

	res, err := client.GetSet(ctx, k, "world")
	assert.Nil(t, err)
	assert.Equal(t, "hello", *res)

	res1, err := client.Get(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, "world", *res1)
}

func TestIncr(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k := "kincr"
	_, err := client.Set(ctx, k, "0")
	assert.Nil(t, err)

	res, err := client.Incr(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), res)
}

func TestIncrBy(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k := "kincrby"
	_, err := client.Set(ctx, k, "0")
	assert.Nil(t, err)

	res, err := client.IncrBy(ctx, k, 2)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), res)

	res, err = client.IncrBy(ctx, k, -3)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), res)
}

func TestIncrByFloat(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	k := "kincrbyfloat"

	res, err := client.IncrByFloat(ctx, k, 2.1)
	assert.Nil(t, err)
	assert.True(t, res-2.1 < 1e-18)

	res, err = client.IncrByFloat(ctx, k, -3.1)
	assert.Nil(t, err)
	assert.True(t, res+1 < 1e-18)
}

func TestMSet(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	kvs := map[string]string{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}
	err := client.MSet(ctx, kvs)
	assert.Nil(t, err)

	res, err := client.MGet(ctx, "k1", "k2", "k3")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, "v1", *res[0])
	assert.Equal(t, "v2", *res[1])
	assert.Equal(t, "v3", *res[2])
}

func TestMSetNx(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()

	kvs := map[string]string{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}

	// TODO: Implement DELETE command to make this work
	// ok, err := client.MSetNX(ctx, kvs)
	// assert.Nil(t, err)
	// assert.True(t, ok)

	ok, err := client.MSetNX(ctx, kvs)
	assert.Nil(t, err)
	assert.False(t, ok)

	// v, err := client.Get(ctx, "k1")
	// assert.Nil(t, err)
	// assert.Equal(t, kvs["k1"], *v)
}

func TestPSetEX(t *testing.T) {
	setupClient()
	defer teardownClient()

	ctx := context.Background()
	err := client.PSetEX(ctx, "k1", "v", 100)
	assert.Nil(t, err)
}
