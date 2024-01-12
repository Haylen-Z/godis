package pkg

import (
	"context"
	"math"

	"log"
	"strconv"
)

type Command interface {
	SendReq(ctx context.Context, protocol Protocol) error
	ReadResp(ctx context.Context, protocol Protocol) (interface{}, error)
}

type Client interface {
	Close() error
	Pipeline() *Pipeline

	// String
	Get(ctx context.Context, key string) (*[]byte, error)
	Set(ctx context.Context, key string, value []byte, args ...arg) (bool, error)
	Append(ctx context.Context, key string, value []byte) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)
	DecrBy(ctx context.Context, key string, decrement int64) (int64, error)
	GetDel(ctx context.Context, key string) (*[]byte, error)
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

func (c *client) exec(ctx context.Context, cmd Command) (res interface{}, err error) {
	var con Connection
	con, err = c.conPool.GetConnection()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			con.SetBroken()
		}
		err = c.conPool.Release(con)
		if err != nil {
			log.Println(err)
		}
	}()
	protocol := c.newProtocol(con)
	err = cmd.SendReq(ctx, protocol)
	if err != nil {
		return
	}
	return cmd.ReadResp(ctx, protocol)
}

type arg func() []string

var NXArg arg = func() []string {
	return []string{"NX"}
}

var XXArg arg = func() []string {
	return []string{"XX"}
}

func EXArg(seconds int) arg {
	return func() []string {
		return []string{"EX", strconv.Itoa(seconds)}
	}
}

func PXArg(miliseconds int) arg {
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

func getArgs(args []arg) [][]byte {
	var res []string
	for _, arg := range args {
		res = append(res, arg()...)
	}
	return stringsToBytes(res)
}
