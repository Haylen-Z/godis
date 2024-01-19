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
	return sendReqWithKey(ctx, protocol, "GET", c.key, nil)
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
	key   string
	value []byte
	args  []arg
}

func (c *stringSetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReqWithKeyValue(ctx, protocol, "SET", c.key, c.value, c.args)
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

func (c *client) Set(ctx context.Context, key string, value []byte, optArgs ...arg) (bool, error) {
	cmd := &stringSetCommand{key: key, value: value, args: optArgs}
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
	return sendReqWithKeyValue(ctx, protocol, "APPEND", c.key, c.value, nil)
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
	return sendReqWithKey(ctx, protocol, "Decr", c.key, nil)
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
	return sendReqWithKeyValue(ctx, protocol, "DECRBY", c.key, []byte(strconv.FormatInt(c.decrement, 10)), nil)
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

type stringGetDelCommand struct {
	key string
}

func (c *stringGetDelCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReqWithKey(ctx, protocol, "GETDEL", c.key, nil)
}

func (c *stringGetDelCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readRespStringOrNil(ctx, protocol)
}

func (c *client) GetDel(ctx context.Context, key string) (*[]byte, error) {
	cmd := &stringGetDelCommand{key: key}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*[]byte), nil
}

type stringGetEXCommand struct {
	key  string
	args []arg
}

func (c *stringGetEXCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReqWithKey(ctx, protocol, "GETEX", c.key, c.args)
}

func (c *stringGetEXCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readRespStringOrNil(ctx, protocol)
}

func (c *client) GetEX(ctx context.Context, key string, optArgs ...arg) (*[]byte, error) {
	cmd := &stringGetEXCommand{key: key, args: optArgs}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*[]byte), nil
}

type stringMGetCommand struct {
	keys []string
}

func (c *stringMGetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReqWithKeys(ctx, protocol, "MGET", c.keys)
}

func (c *stringMGetCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	arr, err := protocol.ReadArray(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*[]byte, 0, len(arr))
	for _, item := range arr {
		if item == nil {
			res = append(res, nil)
		} else {
			res = append(res, item.(*[]byte))
		}
	}
	return res, nil
}

func (c *client) MGet(ctx context.Context, keys ...string) ([]*[]byte, error) {
	cmd := &stringMGetCommand{keys: keys}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.([]*[]byte), nil
}
