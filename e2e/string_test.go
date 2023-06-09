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
