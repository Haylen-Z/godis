package pkg

import (
	"context"
	"net"
	"strconv"

	"github.com/pkg/errors"
)

type StringCommand interface {
	// Get returns the value for the given key.
	Get(ctx context.Context, key string) (*string, error)

	// Set sets the value for the given key.
	Set(ctx context.Context, key, value string, args ...optArg) (bool, error)
}

type Client interface {
	StringCommand
	Close() error
}

type client struct {
	address     string
	connectFunc func(address string) (Connection, error)
}

func NewClient(address string) Client {
	// TODO: add connection pool
	return &client{address: address, connectFunc: NewConnection}
}

func buildCommandAndArgs(cmd string, args ...string) [][]byte {
	cmdAndArgs := make([][]byte, 0, len(args)+1)
	cmdAndArgs = append(cmdAndArgs, []byte(cmd))
	for _, arg := range args {
		cmdAndArgs = append(cmdAndArgs, []byte(arg))
	}
	return cmdAndArgs
}

func (c *client) sendComWithContext(ctx context.Context, sendFunc func(Connection) (interface{}, error)) (interface{}, error) {
	resChan := make(chan interface{})
	errChan := make(chan error)
	go func() {
		con, err := c.connectFunc(c.address)
		if err != nil {
			errChan <- err
			return
		}
		res, err := sendFunc(con)
		if err != nil {
			errChan <- err
			return
		}
		resChan <- res
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errChan:
		return nil, err
	case res := <-resChan:
		return res, nil
	}
}

func (c *client) Get(ctx context.Context, key string) (*string, error) {
	res, err := c.sendComWithContext(ctx, func(con Connection) (interface{}, error) {
		err := con.WriteBulkStringArray(buildCommandAndArgs("GET", key))
		if err != nil {
			return nil, err
		}
		bs, err := con.ReadBulkString()
		if err != nil {
			return nil, err
		}
		if bs == nil {
			return nil, nil
		}
		s := string(*bs)
		return &s, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*string), nil
}

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

func (c *client) Set(ctx context.Context, key, value string, optArgs ...optArg) (bool, error) {
	res, err := c.sendComWithContext(ctx, func(con Connection) (interface{}, error) {
		var args = []string{key, value}
		args = append(args, getArgs(optArgs)...)
		err := con.WriteBulkStringArray(buildCommandAndArgs("SET", args...))
		if err != nil {
			return false, err
		}
		msgType, err := con.GetNextMsgType()
		if err != nil {
			return false, err
		}
		switch msgType {
		case SimpleStringType:
			res, err := con.ReadSimpleString()
			if err != nil {
				return false, err
			}
			if string(res) != "OK" {
				return false, errors.New("unexpected response")
			}
			return true, nil
		case BulkStringType:
			res, err := con.ReadBulkString()
			if err != nil {
				return false, err
			}
			if res != nil {
				return false, errors.New("unexpected response")
			}
			return false, nil
		case ErrorType:
			resErr, err := con.ReadError()
			if err != nil {
				return false, err
			}
			return false, resErr
		default:
			return false, errors.New("unexpected response")
		}

	})
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}

func (c *client) Close() error {
	return nil
}

type Connection interface {
	Protocol
	Close() error
}

type connection struct {
	Protocol
	con net.Conn
}

func (c *connection) Close() error {
	return c.con.Close()
}

func NewConnection(address string) (Connection, error) {
	con, err := net.Dial("tcp", address)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to server")
	}
	return &connection{con: con, Protocol: NewProtocol(con)}, nil
}
