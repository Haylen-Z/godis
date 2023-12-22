package pkg

import (
	"context"
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
	Set(ctx context.Context, key, value string, args ...optArg) (bool, error)
}

type Client interface {
	StringCommand
	Close() error
}

type client struct {
	address       string
	newConnection func(address string) Connection
	newProtocol   func(io.ReadWriter) Protocol
}

func NewClient(address string) Client {
	// TODO: add connection pool
	return &client{address: address, newConnection: NewConnection, newProtocol: NewProtocol}
}

func buildCommandAndArgs(cmd string, args ...string) [][]byte {
	cmdAndArgs := make([][]byte, 0, len(args)+1)
	cmdAndArgs = append(cmdAndArgs, []byte(cmd))
	for _, arg := range args {
		cmdAndArgs = append(cmdAndArgs, []byte(arg))
	}
	return cmdAndArgs
}

type sendCmdFunc func(protocl Protocol, cmdAndArgs [][]byte) (interface{}, error)

type optArg func() []string

func NXArg() []string {
	return []string{"NX"}

}

func XXArg() []string {
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

func getArgs(args []optArg) []string {
	var res []string
	for _, arg := range args {
		res = append(res, arg()...)
	}
	return res
}

func (c *client) sendComWithContext(ctx context.Context, sendFunc sendCmdFunc, cmd string, args ...string) (interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	con := c.newConnection(c.address)
	if err := con.Connect(); err != nil {
		return nil, err
	}
	defer func() {
		err := con.Close()
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

	return sendFunc(c.newProtocol(con), buildCommandAndArgs(cmd, args...))
}

func (c *client) Get(ctx context.Context, key string) (*[]byte, error) {
	get := func(protocl Protocol, cmdAndArgs [][]byte) (interface{}, error) {
		err := protocl.WriteBulkStringArray(cmdAndArgs)
		if err != nil {
			return nil, err
		}
		return protocl.ReadBulkString()
	}

	res, err := c.sendComWithContext(ctx, get, "GET", key)
	if err != nil {
		return nil, err
	}
	return res.(*[]byte), nil
}

func (c *client) Set(ctx context.Context, key, value string, optArgs ...optArg) (bool, error) {
	optArgsargs := getArgs(optArgs)
	args := append([]string{key, value}, optArgsargs...)
	res, err := c.sendComWithContext(ctx, c.set, "SET", args...)
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
