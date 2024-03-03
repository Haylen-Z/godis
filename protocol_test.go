package godis

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func str2BytesPtr(s string) *[]byte {
	b := []byte(s)
	return &b
}

func TestWriteBulkString(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)

	var cases = []struct {
		in  []byte
		out []byte
	}{
		{[]byte("aaabbb123"), []byte("$9\r\naaabbb123\r\n")},
		{[]byte("FWJOI3234--=//"), []byte("$14\r\nFWJOI3234--=//\r\n")},
		{[]byte(""), []byte("$0\r\n\r\n")},
	}

	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()

	for _, c := range cases {
		mkCon.EXPECT().Write(ctx, c.out).Return(0, nil)
		err := proc.WriteBulkString(ctx, c.in)
		assert.Nil(t, err)
	}
}

func TestWriteBulkStringArray(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)

	var cases = []struct {
		in  [][]byte
		out []byte
	}{
		{[][]byte{[]byte("hello"), []byte("world")}, []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")},
		{[][]byte{}, []byte("*0\r\n")},
		{[][]byte{[]byte("121324")}, []byte("*1\r\n$6\r\n121324\r\n")},
	}

	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()
	var out []byte
	for _, c := range cases {
		mkCon.EXPECT().Write(gomock.Any(), gomock.Any()).Return(0, nil).Do(func(ctx context.Context, buf []byte) {
			out = append(out, buf...)
		}).AnyTimes()
		err := proc.WriteBulkStringArray(ctx, c.in)
		assert.Nil(t, err)
		assert.Equal(t, c.out, out)
		out = nil
	}
}

func TestReadBulkString(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)

	var cases = []struct {
		in  []byte
		out []byte
	}{
		{[]byte("$5\r\nhello\r\n$4\r"), []byte("hello")},
		{[]byte("\nkkk1\r\n$"), []byte("kkk1")},
		{[]byte("1\r\no\r\n"), []byte("o")},
		{[]byte("$0\r\n\r\n"), []byte("")},
	}

	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()
	for _, c := range cases {
		mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(c.in), nil).Do(func(ctx context.Context, buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadBulkString(ctx)
		assert.Nil(t, err)
		assert.Equal(t, c.out, *r)
	}

	in := []byte("$7\r\nhello12\r\n$4\r\nkkk1\r\n$1\r\no\r\n$0\r\n\r\n$3\r\n100\r\n")
	outs := [][]byte{[]byte("hello12"), []byte("kkk1"), []byte("o"), []byte(""), []byte("100")}
	mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(in), nil).Do(func(_ context.Context, buf []byte) {
		copy(buf, in)
	}).Times(1)
	for _, out := range outs {
		r, err := proc.ReadBulkString(ctx)
		assert.Nil(t, err)
		assert.Equal(t, out, *r)
	}

	in = []byte("$-1\r\n")
	mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(in), nil).Do(func(_ context.Context, buf []byte) {
		copy(buf, in)
	}).Times(1)
	r, err := proc.ReadBulkString(ctx)
	assert.Nil(t, err)
	assert.Nil(t, r)
}

func TestGetNextMsgType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)

	var cases = []struct {
		in  []byte
		out MsgType
	}{
		{[]byte("*1\r\n$5\r\nhello\r\n"), ArrayType},
		{[]byte("$5\r\nhello\r\n"), BulkStringType},
		{[]byte("+OK\r\n"), SimpleStringType},
		{[]byte("-ERR\r\n"), ErrorType},
		{[]byte(":100\r\n"), IntegerType},
		{[]byte("_\r\n"), NullType},
	}

	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()

	for _, c := range cases {
		mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(c.in), nil).Do(func(_ context.Context, buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.GetNextMsgType(ctx)
		assert.Nil(t, err)
		assert.Equal(t, c.out, r)
		proc.(*respProtocol).hasRecLen = 0
	}
}

func TestReadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)

	var cases = []struct {
		in  []byte
		out Error
	}{
		{[]byte("-ERR unknown command 'foobar'\r\n"), Error{"ERR", "unknown command 'foobar'"}},
		{[]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"), Error{"WRONGTYPE", "Operation against a key holding the wrong kind of value"}},
		{[]byte("-error\r\n"), Error{"", "error"}},
	}

	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()
	for _, c := range cases {
		mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(c.in), nil).Do(func(_ context.Context, buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadError(ctx)
		assert.Nil(t, err)
		assert.Equal(t, c.out, r)
	}

}

func TestReadSimpleString(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)

	var cases = []struct {
		in  []byte
		out string
	}{
		{[]byte("+OK\r\n"), "OK"},
		{[]byte("+PONG\r\n"), "PONG"},
		{[]byte("+QUEUED\r\n"), "QUEUED"},
	}

	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()

	for _, c := range cases {
		mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(c.in), nil).Do(func(_ context.Context, buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadSimpleString(ctx)
		assert.Nil(t, err)
		assert.Equal(t, c.out, string(r))
	}
}

func TestReadInteger(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)

	var cases = []struct {
		in  []byte
		out int64
	}{
		{[]byte(":100\r\n"), 100},
		{[]byte(":-100\r\n"), -100},
		{[]byte(":0\r\n"), 0},
		{[]byte(":1234567890\r\n"), 1234567890},
		{[]byte(":-0\r\n"), 0},
	}

	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()

	for _, c := range cases {
		mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(c.in), nil).Do(func(_ context.Context, buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadInteger(ctx)
		assert.Nil(t, err)
		assert.Equal(t, c.out, r)
	}
}

func TestReadNull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)
	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()

	mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len("_\r\n"), nil).Do(func(_ context.Context, buf []byte) {
		copy(buf, "_\r\n")
	}).Times(1)
	err := proc.ReadNull(ctx)
	assert.Nil(t, err)
}

func TestReadArray(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)
	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()

	cases := []struct {
		in  []byte
		out []interface{}
	}{
		{[]byte("*0\r\n"), []interface{}{}},
		{[]byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"), []interface{}{str2BytesPtr("hello"), str2BytesPtr("world")}},
		{[]byte("*3\r\n:1\r\n:2\r\n:3\r\n"), []interface{}{int64(1), int64(2), int64(3)}},
		{[]byte("*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n"), []interface{}{int64(1), int64(2), int64(3), int64(4), str2BytesPtr("hello")}},
		{[]byte("*-1\r\n"), nil},
		{
			[]byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n"),
			[]interface{}{
				[]interface{}{int64(1), int64(2), int64(3)},
				[]interface{}{[]byte("Hello"), Error{"", "World"}},
			},
		},
		{[]byte("*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n"), []interface{}{str2BytesPtr("hello"), (*[]byte)(nil), str2BytesPtr("world")}},
	}

	for _, c := range cases {
		mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(c.in), nil).Do(func(_ context.Context, buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadArray(ctx)
		assert.Nil(t, err)
		assert.Equal(t, c.out, r)
	}
}

func TestReadMap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mkCon := NewMockConnection(ctrl)
	var proc Protocol = NewProtocol(mkCon)
	ctx := context.Background()

	cases := []struct {
		in  []byte
		out []interface{}
	}{
		{[]byte("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"), []interface{}{[]byte("first"), int64(1), []byte("second"), int64(2)}},
		{[]byte("%2\r\n:1\r\n:1\r\n$5\r\nhello\r\n*3\r\n:1\r\n:2\r\n:3\r\n"),
			[]interface{}{
				int64(1), int64(1), str2BytesPtr("hello"),
				[]interface{}{
					int64(1), int64(2), int64(3),
				},
			},
		},
	}

	for _, c := range cases {
		mkCon.EXPECT().Read(ctx, gomock.Any()).Return(len(c.in), nil).Do(func(_ context.Context, buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadMap(ctx)
		assert.Nil(t, err)
		assert.Equal(t, c.out, r)
	}
}
