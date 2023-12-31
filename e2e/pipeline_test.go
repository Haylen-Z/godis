package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPipeline(t *testing.T) {
	setupClient()
	defer teardownClient()

	pipeline := client.Pipeline()
	ctx := context.Background()

	key := "hello"
	val := "world"

	pipeline.Set(ctx, key, []byte(val))
	pipeline.Get(ctx, key)

	res, err := pipeline.Exec(ctx)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res))
	assert.True(t, res[0].(bool))
	assert.Equal(t, val, string(*res[1].(*[]byte)))

}
