package e2e

import (
	"context"
	"testing"

	"github.com/Haylen-Z/godis"
	"github.com/stretchr/testify/assert"
)

func TestStringPipeline(t *testing.T) {
	setupClient()
	defer teardownClient()

	pipeline := client.Pipeline()
	ctx := context.Background()

	key := "kstringpipeline"
	val := "world"

	pipeline.Set(key, val)
	pipeline.Get(key)
	pipeline.GetEX(key)
	pipeline.GetRange(key, 1, 3)
	pipeline.Append(key, "1")
	pipeline.GetDel(key)
	pipeline.GetSet(key, "oook")

	key1 := "kstringpipeline1"
	_, err := client.Set(ctx, key1, "1")
	assert.Nil(t, err)
	pipeline.Decr(key1)
	pipeline.DecrBy(key1, 2)
	pipeline.Incr(key1)
	pipeline.IncrBy(key1, 2)
	pipeline.IncrByFloat(key1, 2.0)

	lcsk1, lcsk2 := "lcsk1", "lcsk2"
	_, err = client.Set(ctx, lcsk1, "ohmytext")
	assert.Nil(t, err)
	_, err = client.Set(ctx, lcsk2, "mynewtext")
	assert.Nil(t, err)
	pipeline.Lcs(lcsk1, lcsk2)
	pipeline.LcsLen(lcsk1, lcsk2)
	pipeline.LcsIdx(lcsk1, lcsk2)
	pipeline.LcsIdxWithMatchLen(lcsk1, lcsk2)

	msk1, msk2 := "msk1", "msk2"
	msv1, msv2 := "msv1", "msv2"
	pipeline.MSet(map[string]string{msk1: msv1, msk2: msv2})
	pipeline.MGet(msk1, msk2)
	pipeline.MSetNX(map[string]string{msk1: msv1, msk2: msv2})

	pipeline.PSetEX("k", "v", 10)
	pipeline.SetEX("k", "v", 10)
	pipeline.SetNX("k", "v")
	pipeline.SetRange("msk1", 1, "oo")
	pipeline.StrLen("msk1")
	pipeline.SubStr("msk2", 1, -2)

	res, err := pipeline.Exec(ctx)
	assert.Nil(t, err)

	popRes := func() interface{} {
		r := res[0]
		res = res[1:]
		return r
	}

	// Set
	assert.True(t, popRes().(bool))
	// Get
	assert.Equal(t, val, *popRes().(*string))
	// GetEX
	assert.Equal(t, val, *popRes().(*string))
	// GetRange
	assert.Equal(t, "orl", popRes().(string))
	// Append
	assert.Equal(t, int64(6), popRes().(int64))
	// GetDel
	assert.Equal(t, val+"1", string(*popRes().(*string)))
	// GetSet
	assert.Equal(t, (*string)(nil), popRes())
	// Decr
	assert.Equal(t, int64(0), popRes().(int64))
	// DecrBy
	assert.Equal(t, int64(-2), popRes().(int64))
	// Incr
	assert.Equal(t, int64(-1), popRes().(int64))
	// IncrBy
	assert.Equal(t, int64(1), popRes().(int64))
	// IncrByFloat
	assert.True(t, 3.0-popRes().(float64) < 1e-18)
	// Lcs
	assert.Equal(t, "mytext", popRes().(string))
	// LcsLen
	assert.Equal(t, int64(6), popRes().(int64))
	// LcsIdx
	assert.Equal(t, 2, len(popRes().(godis.LcsIdxRes).Matches))
	// LcsIdxWithMatchLen
	assert.Equal(t, 2, popRes().(godis.LcsIdxRes).Matches[1].Len)
	// MSet
	assert.Nil(t, popRes())
	// MGet
	mgRes := popRes().([]*string)
	assert.Equal(t, msv1, *mgRes[0])
	assert.Equal(t, msv2, *mgRes[1])
	// MSetNX
	assert.False(t, popRes().(bool))
	// PSetEX
	assert.Nil(t, popRes())
	// SetEX
	assert.Nil(t, popRes())
	// SetNX
	assert.False(t, popRes().(bool))
	// SetRange
	assert.Equal(t, uint(4), popRes().(uint))
	// StrLen
	assert.Equal(t, uint(4), popRes().(uint))
	// SubStr
	assert.Equal(t, "sv", popRes().(string))
}
