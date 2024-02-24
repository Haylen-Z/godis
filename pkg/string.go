package pkg

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
)

type stringAppendCommand struct {
	key   string
	value string
}

func (c *stringAppendCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"APPEND", c.key, c.value}, nil)
}

func (c *stringAppendCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) Append(ctx context.Context, key string, value string) (int64, error) {
	cmd := &stringAppendCommand{key: key, value: value}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

type stringDecrCommand struct {
	key string
}

func (c *stringDecrCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"Decr", c.key}, nil)
}

func (c *stringDecrCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) Decr(ctx context.Context, key string) (int64, error) {
	cmd := &stringDecrCommand{key: key}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

type stringDecrByCommand struct {
	key       string
	decrement int64
}

func (c *stringDecrByCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"DECRBY", c.key, strconv.FormatInt(c.decrement, 10)}, nil)
}

func (c *stringDecrByCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) DecrBy(ctx context.Context, key string, decrement int64) (int64, error) {
	cmd := &stringDecrByCommand{key: key, decrement: decrement}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

type stringGetCommand struct {
	key string
}

func (c *stringGetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"GET", c.key}, nil)
}

func (c *stringGetCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readRespStringOrNil(ctx, protocol)
}

func (c *client) Get(ctx context.Context, key string) (*string, error) {
	cmd := &stringGetCommand{key: key}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*string), nil
}

type stringGetDelCommand struct {
	key string
}

func (c *stringGetDelCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"GETDEL", c.key}, nil)
}

func (c *stringGetDelCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readRespStringOrNil(ctx, protocol)
}

func (c *client) GetDel(ctx context.Context, key string) (*string, error) {
	cmd := &stringGetDelCommand{key: key}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*string), nil
}

type stringGetEXCommand struct {
	key  string
	args []arg
}

func (c *stringGetEXCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"GETEX", c.key}, c.args)
}

func (c *stringGetEXCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readRespStringOrNil(ctx, protocol)
}

func (c *client) GetEX(ctx context.Context, key string, optArgs ...arg) (*string, error) {
	cmd := &stringGetEXCommand{key: key, args: optArgs}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*string), nil
}

type stringMGetCommand struct {
	keys []string
}

func (c *stringMGetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	args := []string{"MGET"}
	return sendReq(ctx, protocol, append(args, c.keys...), nil)
}

func (c *stringMGetCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	arr, err := protocol.ReadArray(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*string, 0, len(arr))
	for _, item := range arr {
		b := item.(*[]byte)
		if b == nil {
			res = append(res, (*string)(nil))
		} else {
			s := string(*b)
			res = append(res, &s)
		}
	}
	return res, nil
}

func (c *client) MGet(ctx context.Context, keys ...string) ([]*string, error) {
	cmd := &stringMGetCommand{keys: keys}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.([]*string), nil
}

type stringLcsCommand struct {
	key1 string
	key2 string
	args []arg
}

func (c *stringLcsCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"LCS", c.key1, c.key2}, c.args)
}

func (c *stringLcsCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadBulkString(ctx)
	return string(*r), err
}

func (c *client) Lcs(ctx context.Context, key1 string, key2 string, args ...arg) (string, error) {
	cmd := &stringLcsCommand{key1: key1, key2: key2, args: args}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

type stringLcsLenCommand struct {
	key1 string
	key2 string
}

func (c *stringLcsLenCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"LCS", c.key1, c.key2, "LEN"}, nil)
}

func (c *stringLcsLenCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) LcsLen(ctx context.Context, key1 string, key2 string) (int64, error) {
	cmd := &stringLcsLenCommand{key1: key1, key2: key2}
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

func readLcsIdxRes(ctx context.Context, protocol Protocol) (LcsIdxRes, error) {
	t, err := protocol.GetNextMsgType(ctx)
	if err != nil {
		return LcsIdxRes{}, err
	}
	var res []interface{}
	switch t {
	case ArrayType:
		res, err = protocol.ReadArray(ctx)
	case MapType:
		res, err = protocol.ReadMap(ctx)
	default:
		return LcsIdxRes{}, errors.WithStack(errUnexpectedRes)
	}
	if err != nil {
		return LcsIdxRes{}, err
	}
	return NewLcsIdxRes(res)
}

type stringLcsIdxCommand struct {
	key1 string
	key2 string
	args []arg
}

func (c *stringLcsIdxCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"LCS", c.key1, c.key2, "IDX"}, c.args)
}

func (c *stringLcsIdxCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readLcsIdxRes(ctx, protocol)
}

func (c *client) LcsIdx(ctx context.Context, key1 string, key2 string, args ...arg) (LcsIdxRes, error) {
	cmd := &stringLcsIdxCommand{key1: key1, key2: key2, args: args}
	response, err := c.exec(ctx, cmd)
	if err != nil {
		return LcsIdxRes{}, err
	}
	return response.(LcsIdxRes), nil
}

type stringLcsIdxWithMatchLenCommand struct {
	key1 string
	key2 string
	args []arg
}

func (c *stringLcsIdxWithMatchLenCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"LCS", c.key1, c.key2, "IDX", "WITHMATCHLEN"}, c.args)
}

func (c *stringLcsIdxWithMatchLenCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readLcsIdxRes(ctx, protocol)
}

func (c *client) LcsIdxWithMatchLen(ctx context.Context, key1 string, key2 string, args ...arg) (LcsIdxRes, error) {
	cmd := &stringLcsIdxWithMatchLenCommand{key1: key1, key2: key2, args: args}
	response, err := c.exec(ctx, cmd)
	if err != nil {
		return LcsIdxRes{}, err
	}
	return response.(LcsIdxRes), nil
}

type stringGetRangeCommand struct {
	key   string
	start int64
	end   int64
}

func (c *stringGetRangeCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"GETRANGE", c.key, strconv.FormatInt(c.start, 10), strconv.FormatInt(c.end, 10)}, nil)
}

func (c *stringGetRangeCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadBulkString(ctx)
	return string(*r), err
}

func (c *client) GetRange(ctx context.Context, key string, start int64, end int64) (string, error) {
	cmd := &stringGetRangeCommand{key: key, start: start, end: end}
	r, err := c.exec(ctx, cmd)
	if err != nil {
		return "", err
	}
	return r.(string), nil
}

type stringGetSetCommand struct {
	key   string
	value string
}

func (c *stringGetSetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"GETSET", c.key, c.value}, nil)
}

func (c *stringGetSetCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readRespStringOrNil(ctx, protocol)
}

func (c *client) GetSet(ctx context.Context, key string, value string) (*string, error) {
	cmd := &stringGetSetCommand{key: key, value: value}
	r, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return r.(*string), nil
}

type stringIncrCommand struct {
	key string
}

func (c *stringIncrCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"INCR", c.key}, nil)
}

func (c *stringIncrCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) Incr(ctx context.Context, key string) (int64, error) {
	cmd := &stringIncrCommand{key: key}
	r, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

type stringIncrByCommand struct {
	key       string
	increment int64
}

func (c *stringIncrByCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"INCRBY", c.key, strconv.FormatInt(c.increment, 10)}, nil)
}

func (c *stringIncrByCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return protocol.ReadInteger(ctx)
}

func (c *client) IncrBy(ctx context.Context, key string, increment int64) (int64, error) {
	cmd := &stringIncrByCommand{key: key, increment: increment}
	r, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

type stringIncrByFloatCommand struct {
	key       string
	increment float64
}

func (c *stringIncrByFloatCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"INCRBYFLOAT", c.key, strconv.FormatFloat(c.increment, 'f', -1, 64)}, nil)
}

func (c *stringIncrByFloatCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadBulkString(ctx)
	if err != nil {
		return float64(0), err
	}
	return strconv.ParseFloat(string(*r), 64)
}

func (c *client) IncrByFloat(ctx context.Context, key string, increment float64) (float64, error) {
	cmd := &stringIncrByFloatCommand{key: key, increment: increment}
	r, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return r.(float64), nil
}

type stringMSetCommand struct {
	kvs map[string]string
}

func (c *stringMSetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	data := make([]string, 0, len(c.kvs)*2)
	data = append(data, "MSET")
	for k, v := range c.kvs {
		data = append(data, k, v)
	}
	return sendReq(ctx, protocol, data, nil)
}

func (c *stringMSetCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadSimpleString(ctx)
	if err != nil {
		return nil, err
	}
	if string(r) != "OK" {
		return nil, errors.WithStack(errUnexpectedRes)
	}
	return nil, nil
}

func (c *client) MSet(ctx context.Context, kvs map[string]string) error {
	cmd := &stringMSetCommand{kvs: kvs}
	_, err := c.exec(ctx, cmd)
	return err
}

type stringMSetNxCommand struct {
	kvs map[string]string
}

func (c *stringMSetNxCommand) SendReq(ctx context.Context, protocol Protocol) error {
	data := make([]string, 0, len(c.kvs)*2)
	data = append(data, "MSETNX")
	for k, v := range c.kvs {
		data = append(data, k, v)
	}
	return sendReq(ctx, protocol, data, nil)
}

func (c *stringMSetNxCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadInteger(ctx)
	if err != nil {
		return nil, err
	}
	return r == 1, nil
}

func (c *client) MSetNX(ctx context.Context, kvs map[string]string) (bool, error) {
	cmd := &stringMSetNxCommand{kvs: kvs}
	r, err := c.exec(ctx, cmd)
	return r.(bool), err
}

type stringPSetEXCommand struct {
	key          string
	milliseconds uint64
	value        string
}

func (c *stringPSetEXCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"PSETEX", c.key, strconv.FormatUint(c.milliseconds, 10), c.value}, nil)
}

func (c *stringPSetEXCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadSimpleString(ctx)
	if err != nil {
		return nil, err
	}
	if string(r) != "OK" {
		return nil, errors.WithStack(errUnexpectedRes)
	}
	return nil, nil
}

func (c *client) PSetEX(ctx context.Context, key, value string, milliseconds uint64) error {
	cmd := &stringPSetEXCommand{key: key, value: value, milliseconds: milliseconds}
	_, err := c.exec(ctx, cmd)
	return err
}

type stringSetCommand struct {
	key   string
	value string
	args  []arg
}

func (c *stringSetCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"SET", c.key, c.value}, c.args)
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
	case NullType:
		err := protocol.ReadNull(ctx)
		return false, err
	default:
		return false, errors.WithStack(errUnexpectedRes)
	}
}

func (c *client) Set(ctx context.Context, key string, value string, optArgs ...arg) (bool, error) {
	cmd := &stringSetCommand{key: key, value: value, args: optArgs}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return false, err
	}
	return res.(bool), err
}

type stringSetEXCommand struct {
	key     string
	seconds uint64
	value   string
}

func (c *stringSetEXCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"SETEX", c.key, strconv.FormatUint(c.seconds, 10), c.value}, nil)
}

func (c *stringSetEXCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadSimpleString(ctx)
	if err != nil {
		return nil, err
	}
	if string(r) != "OK" {
		return nil, errors.WithStack(errUnexpectedRes)
	}
	return nil, nil
}

func (c *client) SetEX(ctx context.Context, key, value string, seconds uint64) error {
	cmd := &stringSetEXCommand{key: key, value: value, seconds: seconds}
	_, err := c.exec(ctx, cmd)
	return err
}

type stringSetNXCommand struct {
	key   string
	value string
}

func (c *stringSetNXCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"SETNX", c.key, c.value}, nil)
}

func (c *stringSetNXCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadInteger(ctx)
	if err != nil {
		return nil, err
	}
	return r == 1, nil
}

func (c *client) SetNX(ctx context.Context, key string, value string) (bool, error) {
	cmd := &stringSetNXCommand{key: key, value: value}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return false, err
	}
	return res.(bool), err
}

type stringSetRangeCommand struct {
	key    string
	offset uint
	value  string
}

func (c *stringSetRangeCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"SETRANGE", c.key, strconv.FormatUint(uint64(c.offset), 10), c.value}, nil)
}

func (c *stringSetRangeCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadInteger(ctx)
	if err != nil {
		return nil, err
	}
	return uint(r), nil
}

func (c *client) SetRange(ctx context.Context, key string, offset uint, value string) (uint, error) {
	cmd := &stringSetRangeCommand{key: key, value: value, offset: offset}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.(uint), err
}
