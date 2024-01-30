package e2e

import (
	"context"
	"testing"

	"github.com/Haylen-Z/godis/pkg"
	"github.com/stretchr/testify/assert"
)

func TestStringPipeline(t *testing.T) {
	setupClient()
	defer teardownClient()

	pipeline := client.Pipeline()
	ctx := context.Background()

	key := "kstringpipeline"
	val := "world"

	pipeline.Set(key, []byte(val))
	pipeline.Get(key)
	pipeline.GetEX(key)
	pipeline.GetRange(key, 1, 3)
	pipeline.Append(key, []byte("1"))
	pipeline.GetDel(key)
	pipeline.GetSet(key, []byte("oook"))

	key1 := "kstringpipeline1"
	_, err := client.Set(ctx, key1, []byte("1"))
	assert.Nil(t, err)
	pipeline.Decr(key1)
	pipeline.DecrBy(key1, 2)

	lcsk1, lcsk2 := "lcsk1", "lcsk2"
	_, err = client.Set(ctx, lcsk1, []byte("ohmytext"))
	assert.Nil(t, err)
	_, err = client.Set(ctx, lcsk2, []byte("mynewtext"))
	assert.Nil(t, err)
	pipeline.Lcs(lcsk1, lcsk2)
	pipeline.LcsLen(lcsk1, lcsk2)
	pipeline.LcsIdx(lcsk1, lcsk2)
	pipeline.LcsIdxWithMatchLen(lcsk1, lcsk2)

	res, err := pipeline.Exec(ctx)
	assert.Nil(t, err)
	assert.Equal(t, 13, len(res))

	popRes := func() interface{} {
		r := res[0]
		res = res[1:]
		return r
	}

	// Set
	assert.True(t, popRes().(bool))
	// Get
	assert.Equal(t, val, string(*popRes().(*[]byte)))
	// GetEX
	assert.Equal(t, val, string(*popRes().(*[]byte)))
	// GetRange
	assert.Equal(t, "orl", string(popRes().([]byte)))
	// Append
	assert.Equal(t, int64(6), popRes().(int64))
	// GetDel
	assert.Equal(t, val+"1", string(*popRes().(*[]byte)))
	// GetSet
	assert.Equal(t, (*[]byte)(nil), popRes())
	// Decr
	assert.Equal(t, int64(0), popRes().(int64))
	// DecrBy
	assert.Equal(t, int64(-2), popRes().(int64))
	// Lcs
	assert.Equal(t, "mytext", string(popRes().([]byte)))
	// LcsLen
	assert.Equal(t, int64(6), popRes().(int64))
	// LcsIdx
	assert.Equal(t, 2, len(popRes().(pkg.LcsIdxRes).Matches))
	// LcsIdxWithMatchLen
	assert.Equal(t, 2, popRes().(pkg.LcsIdxRes).Matches[1].Len)
}
