package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringPipeline(t *testing.T) {
	setupClient()
	defer teardownClient()

	pipeline := client.Pipeline()
	ctx := context.Background()

	key := "hello"
	val := "world"

	pipeline.Set(key, []byte(val))
	pipeline.Get(key)
	pipeline.Append(key, []byte("1"))
	pipeline.GetDel(key)
	res, err := pipeline.Exec(ctx)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(res))
	assert.True(t, res[0].(bool))
	assert.Equal(t, val, string(*res[1].(*[]byte)))
	assert.Equal(t, int64(6), res[2].(int64))
	assert.Equal(t, val+"1", string(*res[3].(*[]byte)))

	client.Set(ctx, key, []byte("1"))
	pipeline = client.Pipeline()
	pipeline.Decr(key)
	pipeline.DecrBy(key, 2)
	res, err = pipeline.Exec(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), res[0].(int64))
	assert.Equal(t, int64(-2), res[1].(int64))
}
