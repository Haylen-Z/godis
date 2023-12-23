package pkg

import (
	"bytes"
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
	ReadBulkString() (*[]byte, error)
	ReadSimpleString() ([]byte, error)
	ReadError() (Error, error)
	GetNextMsgType() (MsgType, error)

	WriteBulkString(bs []byte) error
	WriteBulkStringArray(bss [][]byte) error
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
	rw        io.ReadWriter
	buf       []byte
	hasRecLen int
}

var buffPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

func NewProtocol(rw io.ReadWriter) Protocol {
	return &respProtocol{rw, buffPool.Get().([]byte), 0}
}

func (p *respProtocol) WriteBulkString(s []byte) error {
	// Bulk string example:"$5\r\nhello\r\n"

	var bs []byte
	bs = append(bs, bulkStringPrefix)
	bs = strconv.AppendInt(bs, int64(len(s)), 10)
	bs = append(bs, terminator...)
	bs = append(bs, s...)
	bs = append(bs, terminator...)
	_, err := p.rw.Write(bs)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (p *respProtocol) readBeforeTerminator() ([]byte, error) {
	rec := make([]byte, p.hasRecLen)
	copy(rec, p.buf[:p.hasRecLen])
	p.hasRecLen = 0

	var err error
	var n int
	for err == nil && !bytes.Contains(rec, terminator) {
		n, err = p.rw.Read(p.buf)
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

func (p *respProtocol) getBulkStringLen() (int, error) {
	rec, err := p.readBeforeTerminator()
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

func (p *respProtocol) ReadBulkString() (*[]byte, error) {
	// Bulk string example:"$5\r\nhello\r\n"

	strLen, err := p.getBulkStringLen()
	if err != nil {
		return nil, err
	}

	if strLen == -1 {
		return nil, nil
	}

	rec, err := p.readBeforeTerminator()
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (p *respProtocol) ReadSimpleString() ([]byte, error) {
	// Simple string example:"+OK\r\n"

	rec, err := p.readBeforeTerminator()
	if err != nil {
		return nil, err
	}
	if len(rec) == 0 || rec[0] != simpleStringPrefix {
		return nil, errors.Wrap(errInvalidMsg, "invalid simple string prefix")
	}
	rec = rec[1:]

	return rec, nil
}

func (p *respProtocol) WriteBulkStringArray(bss [][]byte) error {
	// Bulk string array example:"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"

	pre := []byte{arrayPrefix}
	pre = append(pre, []byte(strconv.FormatInt(int64(len(bss)), 10))...)
	pre = append(pre, terminator...)

	_, err := p.rw.Write(pre)
	if err != nil {
		return errors.WithStack(err)
	}

	for _, bs := range bss {
		err = p.WriteBulkString(bs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *respProtocol) GetNextMsgType() (MsgType, error) {
	// Simple string example:"+OK\r\n"
	// Bulk string example:"$5\r\nhello\r\n"
	// Array example:"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
	// Integer example:":1000\r\n"
	// Error example:"-ERR unknown command 'foobar'\r\n"

	if p.hasRecLen == 0 {
		n, err := p.rw.Read(p.buf)
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

func (p *respProtocol) ReadError() (Error, error) {
	// Error example:"-ERR unknown command 'foobar'\r\n"

	rec, err := p.readBeforeTerminator()
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
