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
	Set(ctx context.Context, key string, value []byte, args ...optArg) (bool, error)
	Append(ctx context.Context, key string, value []byte) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)
	DecrBy(ctx context.Context, key string, decrement int64) (int64, error)
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

func (c *client) Pipeline() *Pipeline {
	return &Pipeline{client: c}
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

type Pipeline struct {
	client   *client
	commands []Command
}

func (p *Pipeline) Get(key string) {
	p.commands = append(p.commands, &stringGetCommand{key: key})
}

func (p *Pipeline) Set(key string, value []byte, args ...optArg) {
	p.commands = append(p.commands, &stringSetCommand{key: key, value: value, optArgs: args})
}

func (p *Pipeline) Exec(ctx context.Context) ([]interface{}, error) {
	r, err := p.client.exec(ctx, p)
	if err != nil {
		return nil, err
	}
	return r.([]interface{}), nil
}

func (p *Pipeline) SendReq(ctx context.Context, protocol Protocol) error {
	for _, cmd := range p.commands {
		err := cmd.SendReq(ctx, protocol)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	var res []interface{}
	for _, cmd := range p.commands {
		r, err := cmd.ReadResp(ctx, protocol)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}
