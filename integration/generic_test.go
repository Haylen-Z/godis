package integration

import (
	"context"
	"testing"

	"github.com/Haylen-Z/godis"
	"github.com/stretchr/testify/assert"
)

func TestCopy(t *testing.T) { run(t, testCopy) }
func testCopy(t *testing.T, client godis.Client, ctx context.Context) {
	k1, k2 := "k1", "k2"
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

func TestDel(t *testing.T) { run(t, testDel) }
func testDel(t *testing.T, client godis.Client, ctx context.Context) {
	k := "k"
	ok, err := client.Set(ctx, k, "v")
	assert.Nil(t, err)
	assert.True(t, ok)

	r, err := client.Del(ctx, k)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), r)

	v, err := client.Get(ctx, k)
	assert.Nil(t, err)
	assert.Nil(t, v)
}
