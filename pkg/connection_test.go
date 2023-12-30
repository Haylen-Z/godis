package pkg

import (
	"context"
	"sync"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestConnectionReadWithCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	con := connection{}

	_, err := con.Read(ctx, []byte{})
	assert.Equal(t, err, ctx.Err())
}

func TestConnectionReadWithDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	dl, _ := ctx.Deadline()
	defer cancel()

	con := connection{con: NewMockConn(ctrl)}
	con.con.(*MockConn).EXPECT().SetReadDeadline(dl).Return(nil).Times(1)
	con.con.(*MockConn).EXPECT().Read(gomock.Any()).Return(0, nil).Times(1)

	_, err := con.Read(ctx, []byte{})
	assert.Nil(t, err)
}

func TestConnectionWriteWithCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	con := connection{}

	_, err := con.Write(ctx, []byte{})
	assert.Equal(t, err, ctx.Err())
}

func TestConnectionWriteWithDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	dl, _ := ctx.Deadline()
	defer cancel()

	con := connection{con: NewMockConn(ctrl)}
	con.con.(*MockConn).EXPECT().SetWriteDeadline(dl).Return(nil).Times(1)
	con.con.(*MockConn).EXPECT().Write(gomock.Any()).Return(0, nil).Times(1)

	_, err := con.Write(ctx, []byte{})
	assert.Nil(t, err)
}

func getMockConnectionPool(ctrl *gomock.Controller) *connectionPool {
	var cp *connectionPool = &connectionPool{
		address:   "1.0.0.1",
		MaxConNum: 10,
		newConnection: func(address string) Connection {
			c := NewMockConnection(ctrl)
			c.EXPECT().Connect().Return(nil).Times(1)
			c.EXPECT().Close().Return(nil).Times(1)
			return c
		},
		mutex: &sync.Mutex{},
	}
	return cp
}

func TestGetConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Get connection
	cp := getMockConnectionPool(ctrl)
	conn, err := cp.GetConnection()
	assert.Nil(t, err)
	assert.IsType(t, &MockConnection{}, conn)
	assert.Equal(t, 1, cp.AllConNum)
	assert.Equal(t, 1, cp.UsedConNum)
	assert.Equal(t, 0, len(cp.pool))

	// Release connection
	err = cp.Release(conn)
	assert.Nil(t, err)
	assert.Equal(t, 0, cp.UsedConNum)
	assert.Equal(t, 1, len(cp.pool))

	// Pool is full
	cons := []Connection{}
	for i := 0; i < cp.MaxConNum; i++ {
		conn, err = cp.GetConnection()
		assert.Nil(t, err)
		cons = append(cons, conn)
	}
	_, err = cp.GetConnection()
	assert.ErrorIs(t, err, ConnectionPoolFullError)
	for _, con := range cons {
		err = cp.Release(con)
		assert.Nil(t, err)
	}

	// Close pool
	err = cp.Close()
	assert.Nil(t, err)
	assert.Equal(t, true, cp.closed)
	assert.Equal(t, 0, cp.AllConNum)
	assert.Equal(t, 0, cp.UsedConNum)
	assert.Equal(t, 0, len(cp.pool))

	// Get connection from closed pool
	_, err = cp.GetConnection()
	assert.ErrorIs(t, err, ClosedPoolError)

	// Release connection to closed pool
	err = cp.Release(conn)
	assert.ErrorIs(t, err, ClosedPoolError)

	// Close closed pool
	err = cp.Close()
	assert.Nil(t, err)
}
