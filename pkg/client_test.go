package pkg

import (
	"context"
	"io"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var mkProtocol *MockProtocol
var testClient Client

func initTestClient(ctr *gomock.Controller) {
	mkCon := NewMockConnection(ctr)
	mkProtocol = NewMockProtocol(ctr)
	mkPool := NewMockConnectionPool(ctr)
	mkPool.EXPECT().GetConnection().Return(mkCon, nil).AnyTimes()
	mkPool.EXPECT().Release(mkCon).Return(nil).AnyTimes()
	newProtocol := func(_ io.ReadWriter) Protocol {
		return mkProtocol
	}

	testClient = &client{address: "1.1.1.1:6379", conPool: mkPool,
		newProtocol: newProtocol,
	}
}

func TestGet(t *testing.T) {
	ctr := gomock.NewController(t)
	defer ctr.Finish()
	initTestClient(ctr)

	key := []byte("key")
	res := []byte("value")
	mkProtocol.EXPECT().WriteBulkStringArray([][]byte{
		[]byte("GET"), key}).Return(nil).Times(1)
	mkProtocol.EXPECT().ReadBulkString().Return(&res, nil).Times(1)

	val, err := testClient.Get(context.TODO(), string(key))
	assert.Nil(t, err)
	assert.Equal(t, res, *val)
}

func TestSetWhileReturnOk(t *testing.T) {
	ctr := gomock.NewController(t)
	defer ctr.Finish()
	initTestClient(ctr)

	key := []byte("key")
	val := []byte("value")

	mkProtocol.EXPECT().WriteBulkStringArray([][]byte{[]byte("SET"), key, val}).Return(nil).Times(1)
	mkProtocol.EXPECT().GetNextMsgType().Return(MsgType(SimpleStringType), nil).Times(1)
	mkProtocol.EXPECT().ReadSimpleString().Return([]byte("OK"), nil).Times(1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	ret, err := testClient.Set(ctx, string(key), val)
	assert.Nil(t, err)
	assert.True(t, ret)

	cancel()
	_, err = testClient.Set(ctx, string(key), val)
	assert.NotNil(t, err)
}
