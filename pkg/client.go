package pkg

import (
	"context"
	"math"

	"log"
	"strconv"

	"github.com/pkg/errors"
)



type Command interface {
	SendReq(ctx context.Context, protocol Protocol) error
	ReadResp(ctx context.Context, protocol Protocol) (interface{}, error)
}


type Client interface {
	Close() error

	// String
	Get(ctx context.Context, key string) (*[]byte, error)
	Set(ctx context.Context, key string, value []byte, args ...optArg) (bool, error)
}

type client struct {
	address     string
	conPool     ConnectionPool
	newProtocol func(Connection) Protocol
}

func NewClient(address string) Client {
	return &client{address: address, conPool: NewConnectionPool(address, math.MaxInt), newProtocol: NewProtocol}
}

func (c *client) Close() error {
	return c.conPool.Close()
}

func (c *client) exec(ctx context.Context, cmd Command) (interface{}, error) {
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
	protocol := c.newProtocol(con)
	err = cmd.SendReq(context.Background(), protocol)
	if err != nil {
		return nil, err
	}
	return cmd.ReadResp(ctx, protocol)
}


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
	key   string
	value []byte
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
