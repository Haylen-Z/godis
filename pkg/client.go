package pkg

import (
	"context"
	"math"

	"io"
	"log"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type StringCommand interface {
	// Get returns the value for the given key.
	Get(ctx context.Context, key string) (*[]byte, error)

	// Set sets the value for the given key.
	Set(ctx context.Context, key string, value []byte, args ...optArg) (bool, error)
}

type Client interface {
	StringCommand
	Close() error
}

type client struct {
	address     string
	conPool     ConnectionPool
	newProtocol func(io.ReadWriter) Protocol
}

func NewClient(address string) Client {
	return &client{address: address, conPool: NewConnectionPool(address, math.MaxInt), newProtocol: NewProtocol}
}

type sendCmdFunc func(protocl Protocol) (interface{}, error)

type optArg func() []string

var NXArg optArg = func() []string {
	return []string{"NX"}
}

var XXArg optArg = func() []string {
	return []string{"XX"}
}

func EXArg(seconds int) optArg {
	return func() []string {
		return []string{"EX", strconv.Itoa(seconds)}
	}
}

func PXArg(miliseconds int) optArg {
	return func() []string {
		return []string{"PX", strconv.Itoa(miliseconds)}
	}
}

func stringsToBytes(strs []string) [][]byte {
	var res [][]byte
	for _, str := range strs {
		res = append(res, []byte(str))
	}
	return res
}

func getArgs(args []optArg) [][]byte {
	var res []string
	for _, arg := range args {
		res = append(res, arg()...)
	}
	return stringsToBytes(res)
}

func (c *client) sendComWithContext(ctx context.Context, sendFunc sendCmdFunc) (interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	con, err := c.conPool.GetConnection()
	if err != nil {
		return nil, err
	}
	defer func() {
		err := c.conPool.Release(con)
		if err != nil {
			log.Println(err)
		}
	}()

	if dl, ok := ctx.Deadline(); ok {
		if c, ok := con.(interface{ SetReadDeadline(t time.Time) error }); ok {
			if err := c.SetReadDeadline(dl); err != nil {
				return nil, err
			}
		}
	}

	return sendFunc(c.newProtocol(con))
}

func (c *client) Get(ctx context.Context, key string) (*[]byte, error) {
	get := func(protocl Protocol) (interface{}, error) {
		data := [][]byte{
			[]byte("GET"),
			[]byte(key),
		}
		err := protocl.WriteBulkStringArray(data)
		if err != nil {
			return nil, err
		}
		return protocl.ReadBulkString()
	}
	res, err := c.sendComWithContext(ctx, get)
	if err != nil {
		return nil, err
	}
	return res.(*[]byte), nil
}

func (c *client) Set(ctx context.Context, key string, value []byte, optArgs ...optArg) (bool, error) {
	args := [][]byte{
		[]byte("SET"),
		[]byte(key),
		value,
	}
	optArgsargs := getArgs(optArgs)
	args = append(args, optArgsargs...)

	com := func(protocl Protocol) (interface{}, error) {
		return c.set(protocl, args)
	}
	res, err := c.sendComWithContext(ctx, com)
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}

func (c *client) set(protocl Protocol, cmdAndArgs [][]byte) (interface{}, error) {
	err := protocl.WriteBulkStringArray(cmdAndArgs)
	if err != nil {
		return false, err
	}
	msgType, err := protocl.GetNextMsgType()
	if err != nil {
		return false, err
	}
	switch msgType {
	case SimpleStringType:
		res, err := protocl.ReadSimpleString()
		if err != nil {
			return false, err
		}
		if string(res) != "OK" {
			return false, errors.New("unexpected response")
		}
		return true, nil
	case BulkStringType:
		res, err := protocl.ReadBulkString()
		if err != nil {
			return false, err
		}
		if res != nil {
			return false, errors.New("unexpected response")
		}
		return false, nil
	case ErrorType:
		resErr, err := protocl.ReadError()
		if err != nil {
			return false, err
		}
		return false, resErr
	default:
		return false, errors.New("unexpected response")
	}
}

func (c *client) Close() error {
	return nil
}
