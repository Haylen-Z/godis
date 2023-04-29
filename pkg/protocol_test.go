package pkg

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestWriteBulkString(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rw_mock := NewMockReadWriter(ctrl)

	var cases = []struct {
		in  []byte
		out []byte
	}{
		{[]byte("aaabbb123"), []byte("$9\r\naaabbb123\r\n")},
		{[]byte("FWJOI3234--=//"), []byte("$14\r\nFWJOI3234--=//\r\n")},
		{[]byte(""), []byte("$0\r\n\r\n")},
	}

	var proc Protocol = NewProtocol(rw_mock)

	for _, c := range cases {
		rw_mock.EXPECT().Write(c.out).Return(0, nil)
		err := proc.WriteBulkString(c.in)
		assert.Nil(t, err)
	}
}

func TestWriteBulkStringArray(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rw_mock := NewMockReadWriter(ctrl)

	var cases = []struct {
		in  [][]byte
		out []byte
	}{
		{[][]byte{[]byte("hello"), []byte("world")}, []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")},
		{[][]byte{}, []byte("*0\r\n")},
		{[][]byte{[]byte("121324")}, []byte("*1\r\n$6\r\n121324\r\n")},
	}

	var proc Protocol = NewProtocol(rw_mock)

	var out []byte
	for _, c := range cases {
		rw_mock.EXPECT().Write(gomock.Any()).Return(0, nil).Do(func(buf []byte) {
			out = append(out, buf...)
		}).AnyTimes()
		err := proc.WriteBulkStringArray(c.in)
		assert.Nil(t, err)
		assert.Equal(t, c.out, out)
		out = nil
	}
}

func TestReadBulkString(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rw_mock := NewMockReadWriter(ctrl)

	var cases = []struct {
		in  []byte
		out []byte
	}{
		{[]byte("$5\r\nhello\r\n$4\r"), []byte("hello")},
		{[]byte("\nkkk1\r\n$"), []byte("kkk1")},
		{[]byte("1\r\no\r\n"), []byte("o")},
		{[]byte("$0\r\n\r\n"), []byte("")},
	}

	var proc Protocol = NewProtocol(rw_mock)
	for _, c := range cases {
		rw_mock.EXPECT().Read(gomock.Any()).Return(len(c.in), nil).Do(func(buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadBulkString()
		assert.Nil(t, err)
		assert.Equal(t, c.out, *r)
	}

	in := []byte("$7\r\nhello12\r\n$4\r\nkkk1\r\n$1\r\no\r\n$0\r\n\r\n$3\r\n100\r\n")
	outs := [][]byte{[]byte("hello12"), []byte("kkk1"), []byte("o"), []byte(""), []byte("100")}
	rw_mock.EXPECT().Read(gomock.Any()).Return(len(in), nil).Do(func(buf []byte) {
		copy(buf, in)
	}).Times(1)
	for _, out := range outs {
		r, err := proc.ReadBulkString()
		assert.Nil(t, err)
		assert.Equal(t, out, *r)
	}

	in = []byte("$-1\r\n")
	rw_mock.EXPECT().Read(gomock.Any()).Return(len(in), nil).Do(func(buf []byte) {
		copy(buf, in)
	}).Times(1)
	r, err := proc.ReadBulkString()
	assert.Nil(t, err)
	assert.Nil(t, r)
}

func TestGetNextMsgType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rw_mock := NewMockReadWriter(ctrl)

	var cases = []struct {
		in  []byte
		out MsgType
	}{
		{[]byte("*1\r\n$5\r\nhello\r\n"), ArrayType},
		{[]byte("$5\r\nhello\r\n"), BulkStringType},
		{[]byte("+OK\r\n"), SimpleStringType},
		{[]byte("-ERR\r\n"), ErrorType},
		{[]byte(":100\r\n"), IntegerType},
	}

	var proc Protocol = NewProtocol(rw_mock)

	for _, c := range cases {
		rw_mock.EXPECT().Read(gomock.Any()).Return(len(c.in), nil).Do(func(buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.GetNextMsgType()
		assert.Nil(t, err)
		assert.Equal(t, c.out, r)
		proc.(*respProtocol).hasRecLen = 0
	}
}

func TestReadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rw_mock := NewMockReadWriter(ctrl)

	var cases = []struct {
		in  []byte
		out Error
	}{
		{[]byte("-ERR unknown command 'foobar'\r\n"), Error{"ERR", "unknown command 'foobar'"}},
		{[]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"), Error{"WRONGTYPE", "Operation against a key holding the wrong kind of value"}},
	}

	var proc Protocol = NewProtocol(rw_mock)
	for _, c := range cases {
		rw_mock.EXPECT().Read(gomock.Any()).Return(len(c.in), nil).Do(func(buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadError()
		assert.Nil(t, err)
		assert.Equal(t, c.out, r)
	}

}

func TestReadSimpleString(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rw_mock := NewMockReadWriter(ctrl)

	var cases = []struct {
		in  []byte
		out string
	}{
		{[]byte("+OK\r\n"), "OK"},
		{[]byte("+PONG\r\n"), "PONG"},
		{[]byte("+QUEUED\r\n"), "QUEUED"},
	}

	var proc Protocol = NewProtocol(rw_mock)
	for _, c := range cases {
		rw_mock.EXPECT().Read(gomock.Any()).Return(len(c.in), nil).Do(func(buf []byte) {
			copy(buf, c.in)
		}).Times(1)
		r, err := proc.ReadSimpleString()
		assert.Nil(t, err)
		assert.Equal(t, c.out, string(r))
	}
}
