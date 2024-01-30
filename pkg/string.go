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
	return readRespStringOrNil(ctx, protocol)
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
	case NullType:
		err := protocol.ReadNull(ctx)
		return (*[]byte)(nil), err
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
	return sendReqWithKeys(ctx, protocol, "LCS", []string{c.key1, c.key2}, c.args...)
}

func (c *stringLcsCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadBulkString(ctx)
	if err != nil {
		return nil, err
	}
	return *r, nil
}

func (c *client) Lcs(ctx context.Context, key1 string, key2 string, args ...arg) ([]byte, error) {
	cmd := &stringLcsCommand{key1: key1, key2: key2, args: args}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.([]byte), nil
}

type stringLcsLenCommand struct {
	key1 string
	key2 string
}

func (c *stringLcsLenCommand) SendReq(ctx context.Context, protocol Protocol) error {
	LEN := func() []string {
		return []string{"LEN"}
	}
	return sendReqWithKeys(ctx, protocol, "LCS", []string{c.key1, c.key2}, LEN)
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
	IDX := func() []string {
		return []string{"IDX"}
	}
	return sendReqWithKeys(ctx, protocol, "LCS", []string{c.key1, c.key2}, append(c.args, IDX)...)
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
	IDX_WITHMATCHLEN := func() []string {
		return []string{"IDX", "WITHMATCHLEN"}
	}
	return sendReqWithKeys(ctx, protocol, "LCS", []string{c.key1, c.key2}, append(c.args, IDX_WITHMATCHLEN)...)
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
	a := func() []string {
		return []string{strconv.FormatInt(c.start, 10), strconv.FormatInt(c.end, 10)}
	}
	return sendReqWithKey(ctx, protocol, "GETRANGE", c.key, []arg{a})
}

func (c *stringGetRangeCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	return readRespStringOrNil(ctx, protocol)
}

func (c *client) GetRange(ctx context.Context, key string, start int64, end int64) (*[]byte, error) {
	cmd := &stringGetRangeCommand{key: key, start: start, end: end}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*[]byte), nil
}
