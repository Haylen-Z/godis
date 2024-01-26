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
	SimpleStringType MsgType = iota
	BulkStringType
	ArrayType
	IntegerType
	ErrorType
	NullType
	MapType
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
	ReadInteger(ctx context.Context) (int64, error)
	ReadNull(ctx context.Context) error
	ReadArray(ctx context.Context) ([]interface{}, error)
	ReadMap(ctx context.Context) ([]interface{}, error)

	WriteBulkString(ctx context.Context, bs []byte) error
	WriteBulkStringArray(ctx context.Context, bss [][]byte) error
}

const (
	bulkStringPrefix   = '$'
	arrayPrefix        = '*'
	simpleStringPrefix = '+'
	errorPrefix        = '-'
	integerPrefix      = ':'
	nullPrefix         = '_'
	mapPrefix          = '%'
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
		p.con.SetBroken()
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
		return nil, errors.Wrap(err, "failed to read from connection")
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
	// Null: _\r\n

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
	case nullPrefix:
		return NullType, nil
	case mapPrefix:
		return MapType, nil
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
	var errType string
	if idx == -1 {
		errType = ""
		idx = 0
	} else {
		errType = string(rec[:idx])
	}

	errMsg := string(bytes.TrimPrefix(rec[idx:], []byte{' '}))
	return Error{errType, errMsg}, nil
}

func (p *respProtocol) ReadInteger(ctx context.Context) (int64, error) {
	// Integer example:":1000\r\n" ":+10\r\n" ":-1000\r\n"

	rec, err := p.readBeforeTerminator(ctx)
	if err != nil {
		return 0, err
	}
	if len(rec) == 0 || rec[0] != integerPrefix {
		return 0, errors.Wrap(errInvalidMsg, "invalid integer prefix")
	}
	rec = rec[1:]

	return strconv.ParseInt(string(rec), 10, 64)
}

func (p *respProtocol) ReadNull(ctx context.Context) error {
	// Null: _\r\n

	rec, err := p.readBeforeTerminator(ctx)
	if err != nil {
		return err
	}
	if len(rec) == 0 || rec[0] != nullPrefix {
		return errors.Wrap(errInvalidMsg, "invalid null prefix")
	}
	return nil
}

func (p *respProtocol) ReadArray(ctx context.Context) ([]interface{}, error) {
	// *<number-of-elements>\r\n<element-1>...<element-n>

	rec, err := p.readBeforeTerminator(ctx)
	if err != nil {
		return nil, err
	}
	if len(rec) == 0 || rec[0] != arrayPrefix {
		return nil, errors.Wrap(errInvalidMsg, "invalid array prefix")
	}
	rec = rec[1:]

	arrayLen, err := strconv.ParseInt(string(rec), 10, 64)
	if err != nil {
		return nil, errors.Wrap(errInvalidMsg, "invalid array length")
	}
	if arrayLen == -1 {
		return nil, nil
	}

	res := make([]interface{}, 0, arrayLen)
	for i := 0; i < int(arrayLen); i++ {
		t, err := p.GetNextMsgType(ctx)
		if err != nil {
			return nil, err
		}

		var r interface{}
		switch t {
		case SimpleStringType:
			r, err = p.ReadSimpleString(ctx)
		case BulkStringType:
			r, err = p.ReadBulkString(ctx)
		case ArrayType:
			r, err = p.ReadArray(ctx)
		case IntegerType:
			r, err = p.ReadInteger(ctx)
		case ErrorType:
			r, err = p.ReadError(ctx)
		case MapType:
			r, err = p.ReadMap(ctx)
		case NullType:
			err = p.ReadNull(ctx)
		default:
			return nil, errors.Wrap(errInvalidMsg, "invalid msg type")
		}
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (p *respProtocol) ReadMap(ctx context.Context) ([]interface{}, error) {
	// %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>

	rec, err := p.readBeforeTerminator(ctx)
	if err != nil {
		return nil, err
	}
	if len(rec) == 0 || rec[0] != mapPrefix {
		return nil, errors.Wrap(errInvalidMsg, "invalid map prefix")
	}
	rec = rec[1:]

	itemLen, err := strconv.ParseInt(string(rec), 10, 64)
	if err != nil {
		return nil, errors.Wrap(errInvalidMsg, "invalid map length")
	}
	if itemLen == -1 {
		return nil, nil
	}

	res := make([]interface{}, 0, itemLen*2)
	for i := 0; i < int(itemLen*2); i++ {
		t, err := p.GetNextMsgType(ctx)
		if err != nil {
			return nil, err
		}

		var r interface{}
		switch t {
		case SimpleStringType:
			r, err = p.ReadSimpleString(ctx)
		case BulkStringType:
			r, err = p.ReadBulkString(ctx)
		case ArrayType:
			r, err = p.ReadArray(ctx)
		case IntegerType:
			r, err = p.ReadInteger(ctx)
		case ErrorType:
			r, err = p.ReadError(ctx)
		case MapType:
			r, err = p.ReadMap(ctx)
		case NullType:
			err = p.ReadNull(ctx)
		default:
			return nil, errors.Wrap(errInvalidMsg, "invalid msg type")
		}
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}
