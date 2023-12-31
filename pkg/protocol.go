package pkg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/pkg/errors"
)

type MsgType int

const (
	SimpleStringType = iota
	BulkStringType
	ArrayType
	IntegerType
	ErrorType
)

var errInvalidMsg = fmt.Errorf("invalid msg type")

type Error struct {
	Type string
	Msg  string
}

func (e Error) Error() string {
	return e.Type + ": " + e.Msg
}

type Protocol interface {
	ReadBulkString(ctx context.Context) (*[]byte, error)
	ReadSimpleString(ctx context.Context) ([]byte, error)
	ReadError(ctx context.Context) (Error, error)
	GetNextMsgType(ctx context.Context) (MsgType, error)

	WriteBulkString(ctx context.Context, bs []byte) error
	WriteBulkStringArray(ctx context.Context, bss [][]byte) error
}

const (
	bulkStringPrefix   = '$'
	arrayPrefix        = '*'
	simpleStringPrefix = '+'
	errorPrefix        = '-'
	integerPrefix      = ':'
)

var terminator = []byte{'\r', '\n'}

// Implement RESP protocol
// https://redis.io/docs/reference/protocol-spec
type respProtocol struct {
	con       Connection
	buf       []byte
	hasRecLen int
}

var buffPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

func NewProtocol(c Connection) Protocol {
	return &respProtocol{c, buffPool.Get().([]byte), 0}
}

func (p *respProtocol) WriteBulkString(ctx context.Context, s []byte) error {
	// Bulk string example:"$5\r\nhello\r\n"

	var bs []byte
	bs = append(bs, bulkStringPrefix)
	bs = strconv.AppendInt(bs, int64(len(s)), 10)
	bs = append(bs, terminator...)
	bs = append(bs, s...)
	bs = append(bs, terminator...)
	_, err := p.con.Write(ctx, bs)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (p *respProtocol) readBeforeTerminator(ctx context.Context) ([]byte, error) {
	rec := make([]byte, p.hasRecLen)
	copy(rec, p.buf[:p.hasRecLen])
	p.hasRecLen = 0

	var err error
	var n int
	for err == nil && !bytes.Contains(rec, terminator) {
		n, err = p.con.Read(ctx, p.buf)
		rec = append(rec, p.buf[:n]...)
	}
	if err != nil && err != io.EOF {
		return nil, errors.WithStack(err)
	}

	terIdx := bytes.Index(rec, terminator)
	copy(p.buf, rec[terIdx+2:])
	p.hasRecLen = len(rec[terIdx+2:])

	res := make([]byte, terIdx)
	copy(res, rec[:terIdx])
	return res, nil
}

func (p *respProtocol) getBulkStringLen(ctx context.Context) (int, error) {
	rec, err := p.readBeforeTerminator(ctx)
	if err != nil {
		return 0, err
	}

	if len(rec) == 0 || rec[0] != bulkStringPrefix {
		return 0, errors.Wrap(errInvalidMsg, "invalid bulk string prefix")
	}
	rec = rec[1:]

	strLen, err := strconv.ParseInt(string(rec), 10, 64)
	if err != nil {
		return 0, errors.Wrap(errInvalidMsg, "invalid bulk string length")
	}

	return int(strLen), nil
}

func (p *respProtocol) ReadBulkString(ctx context.Context) (*[]byte, error) {
	// Bulk string example:"$5\r\nhello\r\n"

	strLen, err := p.getBulkStringLen(ctx)
	if err != nil {
		return nil, err
	}

	if strLen == -1 {
		return nil, nil
	}

	rec, err := p.readBeforeTerminator(ctx)
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (p *respProtocol) ReadSimpleString(ctx context.Context) ([]byte, error) {
	// Simple string example:"+OK\r\n"

	rec, err := p.readBeforeTerminator(ctx)
	if err != nil {
		return nil, err
	}
	if len(rec) == 0 || rec[0] != simpleStringPrefix {
		return nil, errors.Wrap(errInvalidMsg, "invalid simple string prefix")
	}
	rec = rec[1:]

	return rec, nil
}

func (p *respProtocol) WriteBulkStringArray(ctx context.Context, bss [][]byte) error {
	// Bulk string array example:"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"

	pre := []byte{arrayPrefix}
	pre = append(pre, []byte(strconv.FormatInt(int64(len(bss)), 10))...)
	pre = append(pre, terminator...)

	_, err := p.con.Write(ctx, pre)
	if err != nil {
		return errors.WithStack(err)
	}

	for _, bs := range bss {
		err = p.WriteBulkString(ctx, bs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *respProtocol) GetNextMsgType(ctx context.Context) (MsgType, error) {
	// Simple string example:"+OK\r\n"
	// Bulk string example:"$5\r\nhello\r\n"
	// Array example:"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
	// Integer example:":1000\r\n"
	// Error example:"-ERR unknown command 'foobar'\r\n"

	if p.hasRecLen == 0 {
		n, err := p.con.Read(ctx, p.buf)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		p.hasRecLen = n
	}

	switch p.buf[0] {
	case simpleStringPrefix:
		return SimpleStringType, nil
	case bulkStringPrefix:
		return BulkStringType, nil
	case arrayPrefix:
		return ArrayType, nil
	case integerPrefix:
		return IntegerType, nil
	case errorPrefix:
		return ErrorType, nil
	default:
		return 0, errors.WithStack(errInvalidMsg)
	}
}

func (p *respProtocol) ReadError(ctx context.Context) (Error, error) {
	// Error example:"-ERR unknown command 'foobar'\r\n"

	rec, err := p.readBeforeTerminator(ctx)
	if err != nil {
		return Error{}, err
	}
	if len(rec) == 0 || rec[0] != errorPrefix {
		return Error{}, errors.Wrap(errInvalidMsg, "invalid error prefix")
	}
	rec = rec[1:]

	idx := bytes.Index(rec, []byte{' '})
	if idx == -1 {
		return Error{}, errors.Wrap(errInvalidMsg, "invalid error prefix")
	}

	errType := string(rec[:idx])
	errMsg := string(bytes.TrimPrefix(rec[idx:], []byte{' '}))
	return Error{errType, errMsg}, nil
}
