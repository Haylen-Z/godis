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
	pipeline.Append(key, []byte("1"))
	pipeline.GetDel(key)

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
	assert.Equal(t, 11, len(res))

	// Set
	assert.True(t, res[0].(bool))
	// Get
	assert.Equal(t, val, string(*res[1].(*[]byte)))
	// GetEX
	assert.Equal(t, val, string(*res[2].(*[]byte)))
	// Append
	assert.Equal(t, int64(6), res[3].(int64))
	// GetDel
	assert.Equal(t, val+"1", string(*res[4].(*[]byte)))
	// Decr
	assert.Equal(t, int64(0), res[5].(int64))
	// DecrBy
	assert.Equal(t, int64(-2), res[6].(int64))
	// Lcs
	assert.Equal(t, "mytext", string(res[7].([]byte)))
	// LcsLen
	assert.Equal(t, int64(6), res[8].(int64))
	// LcsIdx
	assert.Equal(t, 2, len(res[9].(pkg.LcsIdxRes).Matches))
	// LcsIdxWithMatchLen
	assert.Equal(t, 2, res[10].(pkg.LcsIdxRes).Matches[1].Len)
}
