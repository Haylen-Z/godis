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
			return false, errors.WithStack(errUnexpectedRes)
		}
		return true, nil
	case BulkStringType:
		res, err := protocol.ReadBulkString(ctx)
		if err != nil {
			return false, err
		}
		if res != nil {
			return false, errors.WithStack(errUnexpectedRes)
		}
		return false, nil
	case ErrorType:
		resErr, err := protocol.ReadError(ctx)
		if err != nil {
			return false, err
		}
		return false, resErr
	default:
		return false, errors.WithStack(errUnexpectedRes)
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

type stringLcsCommand struct {
	key1 string
	key2 string
	args []arg
}

func (c *stringLcsCommand) SendReq(ctx context.Context, protocol Protocol) error {
	data := [][]byte{
		[]byte("LCS"),
		[]byte(c.key1),
		[]byte(c.key2),
	}
	data = append(data, getArgs(c.args)...)
	return protocol.WriteBulkStringArray(ctx, data)
}

func (c *stringLcsCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	msgType, err := protocol.GetNextMsgType(ctx)
	if err != nil {
		return nil, err
	}
	switch msgType {
	case BulkStringType:
		return protocol.ReadBulkString(ctx)
	case IntegerType:
		return protocol.ReadInteger(ctx)
	case ArrayType:
		return protocol.ReadArray(ctx)
	case MapType:
		return protocol.ReadMap(ctx)
	case ErrorType:
		err1, err := protocol.ReadError(ctx)
		if err != nil {
			return nil, err
		}
		return nil, err1
	default:
		return nil, errors.WithStack(errUnexpectedRes)
	}
}

func (c *client) Lcs(ctx context.Context, key1 string, key2 string, args ...arg) ([]byte, error) {
	cmd := &stringLcsCommand{key1: key1, key2: key2, args: args}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return *res.(*[]byte), nil
}

func (c *client) LcsLen(ctx context.Context, key1 string, key2 string) (int64, error) {
	LEN := func() []string {
		return []string{"LEN"}
	}
	cmd := &stringLcsCommand{key1: key1, key2: key2, args: []arg{LEN}}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

type LcsIdxMatch struct {
	Pos1 [2]int
	Pos2 [2]int
	Len  int
}

func NewLcsIdxMatch(raw []interface{}) (LcsIdxMatch, error) {
	match := LcsIdxMatch{}
	if len(raw) < 2 {
		return match, errors.WithStack(errUnexpectedRes)
	}
	pos1, ok := raw[0].([]interface{})
	if !ok {
		return match, errors.WithStack(errUnexpectedRes)
	}
	if len(pos1) != 2 {
		return match, errors.WithStack(errUnexpectedRes)
	}
	pos2, ok := raw[1].([]interface{})
	if !ok {
		return match, errors.WithStack(errUnexpectedRes)
	}
	if len(pos2) != 2 {
		return match, errors.WithStack(errUnexpectedRes)
	}
	if len(raw) > 2 {
		match.Len = int(raw[2].(int64))
	}
	match.Pos1 = [2]int{int(pos1[0].(int64)), int(pos1[1].(int64))}
	match.Pos2 = [2]int{int(pos2[0].(int64)), int(pos2[1].(int64))}
	return match, nil
}

type LcsIdxRes struct {
	Matches []LcsIdxMatch
	Len     int64
}

func NewLcsIdxRes(raw []interface{}) (LcsIdxRes, error) {
	idx := LcsIdxRes{}
	if len(raw) != 4 {
		return idx, errors.WithStack(errUnexpectedRes)
	}
	matches, ok := raw[1].([]interface{})
	if !ok {
		return idx, errors.WithStack(errUnexpectedRes)
	}
	idx.Matches = make([]LcsIdxMatch, 0, len(matches))
	for _, matchRaw := range matches {
		matchArr, ok := matchRaw.([]interface{})
		if !ok {
			return idx, errors.WithStack(errUnexpectedRes)
		}
		match, err := NewLcsIdxMatch(matchArr)
		if err != nil {
			return idx, err
		}
		idx.Matches = append(idx.Matches, match)
	}
	idx.Len, ok = raw[3].(int64)
	if !ok {
		return idx, errors.WithStack(errUnexpectedRes)
	}
	return idx, nil
}

func (c *client) LcsIdx(ctx context.Context, key1 string, key2 string, args ...arg) (LcsIdxRes, error) {
	IDX := func() []string {
		return []string{"IDX"}
	}
	cmd := &stringLcsCommand{key1: key1, key2: key2, args: append(args, IDX)}
	raw, err := c.exec(ctx, cmd)
	if err != nil {
		return LcsIdxRes{}, err
	}
	res := raw.([]interface{})
	return NewLcsIdxRes(res)
}

func (c *client) LcsIdxWithMatchLen(ctx context.Context, key1 string, key2 string, args ...arg) (LcsIdxRes, error) {
	IdxWithMatchLen := func() []string {
		return []string{"IDX", "WITHMATCHLEN"}
	}
	cmd := &stringLcsCommand{key1: key1, key2: key2, args: append(args, IdxWithMatchLen)}
	raw, err := c.exec(ctx, cmd)
	if err != nil {
		return LcsIdxRes{}, err
	}
	res := raw.([]interface{})
	return NewLcsIdxRes(res)
}
