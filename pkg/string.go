package pkg

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
)

type stringGetCommand struct {
	key string
}

func (c *stringGetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	data := [][]byte{
		[]byte("GET"),
		[]byte(c.key),
	}
	return protocol.WriteBulkStringArray(ctx, data)
}

func (c *stringGetCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadBulkString(ctx)
}

func (c *client) Get(ctx context.Context, key string) (*[]byte, error) {
	cmd := &stringGetCommand{key: key}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*[]byte), nil
}

type stringSetCommand struct {
	key     string
	value   []byte
	optArgs []optArg
}

func (c *stringSetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	data := [][]byte{
		[]byte("SET"),
		[]byte(c.key),
		c.value,
	}
	data = append(data, getArgs(c.optArgs)...)
	return protocol.WriteBulkStringArray(ctx, data)
}

func (c *stringSetCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	msgType, err := protocol.GetNextMsgType(ctx)
	if err != nil {
		return false, err
	}
	switch msgType {
	case SimpleStringType:
		res, err := protocol.ReadSimpleString(ctx)
		if err != nil {
			return false, err
		}
		if string(res) != "OK" {
			return false, errors.New("unexpected response")
		}
		return true, nil
	case BulkStringType:
		res, err := protocol.ReadBulkString(ctx)
		if err != nil {
			return false, err
		}
		if res != nil {
			return false, errors.New("unexpected response")
		}
		return false, nil
	case ErrorType:
		resErr, err := protocol.ReadError(ctx)
		if err != nil {
			return false, err
		}
		return false, resErr
	default:
		return false, errors.New("unexpected response")
	}
}

func (c *client) Set(ctx context.Context, key string, value []byte, optArgs ...optArg) (bool, error) {
	cmd := &stringSetCommand{key: key, value: value, optArgs: optArgs}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}

type stringAppendCommand struct {
	key   string
	value []byte
}

func (c *stringAppendCommand) SendReq(ctx context.Context, protocol Protocol) error {
	data := [][]byte{
		[]byte("APPEND"),
		[]byte(c.key),
		c.value,
	}
	return protocol.WriteBulkStringArray(ctx, data)
}

func (c *stringAppendCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) Append(ctx context.Context, key string, value []byte) (int64, error) {
	cmd := &stringAppendCommand{key: key, value: value}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

type integerResCommand struct {
	key string
}

func (c *integerResCommand) SendReq(ctx context.Context, protocol Protocol) error {
	data := [][]byte{
		[]byte("DECR"),
		[]byte(c.key),
	}
	return protocol.WriteBulkStringArray(ctx, data)
}

func (c *integerResCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) Decr(ctx context.Context, key string) (int64, error) {
	cmd := &integerResCommand{key: key}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

type integerDecrByCommand struct {
	key       string
	decrement int64
}

func (c *integerDecrByCommand) SendReq(ctx context.Context, protocol Protocol) error {
	data := [][]byte{
		[]byte("DECRBY"),
		[]byte(c.key),
		[]byte(strconv.FormatInt(c.decrement, 10)),
	}
	return protocol.WriteBulkStringArray(ctx, data)
}

func (c *integerDecrByCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) DecrBy(ctx context.Context, key string, decrement int64) (int64, error) {
	cmd := &integerDecrByCommand{key: key, decrement: decrement}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}
