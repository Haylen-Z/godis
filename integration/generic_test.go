package integration

import (
	"context"
	"testing"

	"github.com/Haylen-Z/godis"
	"github.com/stretchr/testify/assert"
)

func TestCopy(t *testing.T) { Run(t, testCopy) }
func testCopy(t *testing.T, client godis.Client) {
	k1, k2 := "k1", "k2"
	ctx := context.Background()

	ok, err := client.Set(ctx, k1, "v1")
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = client.Copy(ctx, k1, k2, godis.REPLACEArg)
	assert.Nil(t, err)
	assert.True(t, ok)

	v, err := client.Get(ctx, k2)
	assert.Nil(t, err)
	assert.Equal(t, "v1", *v)

	ok, err = client.Set(ctx, k2, "v2")
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = client.Copy(ctx, k2, k1)
	assert.Nil(t, err)
	assert.False(t, ok)
}
