package pkg

import (
	"context"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

var mkProtocol *MockProtocol
var testClient Client

func initTestClient(ctr *gomock.Controller) {
	mkCon := NewMockConnection(ctr)
	mkProtocol = NewMockProtocol(ctr)
	mkPool := NewMockConnectionPool(ctr)
	mkPool.EXPECT().GetConnection().Return(mkCon, nil).AnyTimes()
	mkPool.EXPECT().Release(mkCon).Return(nil).AnyTimes()
	newProtocol := func(_ Connection) Protocol {
		return mkProtocol
	}

	config := &ClientConfig{
		Address: "1.1.1.1:6379",
	}
	testClient = &client{config: config, conPool: mkPool,
		newProtocol: newProtocol,
	}
}

func TestPipeline(t *testing.T) {
	ctr := gomock.NewController(t)
	defer ctr.Finish()
	initTestClient(ctr)

	key := []byte("key")
	val := []byte("value")
	ctx := context.Background()

	// Set
	mkProtocol.EXPECT().WriteBulkStringArray(ctx, [][]byte{[]byte("SET"), key, val}).Return(nil).Times(1)
	mkProtocol.EXPECT().GetNextMsgType(ctx).Return(SimpleStringType, nil).Times(2)
	mkProtocol.EXPECT().GetNextMsgType(ctx).Return(BulkStringType, nil).Times(1)
	mkProtocol.EXPECT().ReadSimpleString(ctx).Return([]byte("OK"), nil).Times(1)

	// Get
	mkProtocol.EXPECT().WriteBulkStringArray(ctx, [][]byte{
		[]byte("GET"), key}).Return(nil).Times(1)
	mkProtocol.EXPECT().ReadBulkString(ctx).Return(&val, nil).Times(1)

	pipeline := testClient.Pipeline()
	pipeline.Set(string(key), string(val))
	pipeline.Get(string(key))
	res, err := pipeline.Exec(ctx)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res))

	// Set result
	assert.True(t, res[0].(bool))

	// Get result
	assert.Equal(t, string(val), *res[1].(*string))
}
