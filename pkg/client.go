package pkg

import (
	"context"
	"fmt"
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
	con      net.Conn
	protocol Protocol
}

func NewClient(address string) (Client, error) {
	con, err := net.Dial("tcp", address)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to server")
	}
	return &client{con: con, protocol: NewProtocl(con)}, nil
}

func buildCommandAndArgs(cmd string, args ...string) [][]byte {
	cmdAndArgs := make([][]byte, 0, len(args)+1)
	cmdAndArgs = append(cmdAndArgs, []byte(cmd))
	for _, arg := range args {
		cmdAndArgs = append(cmdAndArgs, []byte(arg))
	}
	return cmdAndArgs
}

func (c *client) Get(ctx context.Context, key string) (*string, error) {
	resChan := make(chan *string)
	errChan := make(chan error)

	go func() {
		err := c.protocol.WriteBulkStringArray(buildCommandAndArgs("GET", key))
		if err != nil {
			errChan <- err
			return
		}

		bs, err := c.protocol.ReadBulkString()
		if err != nil {
			errChan <- err
			return
		}

		if bs == nil {
			resChan <- nil
			return
		}
		res := string(*bs)
		resChan <- &res
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
	resChan := make(chan bool)
	errChan := make(chan error)

	var args = []string{key, value}
	args = append(args, getArgs(optArgs)...)
	fmt.Println(args)

	go func() {
		err := c.protocol.WriteBulkStringArray(buildCommandAndArgs("SET", args...))
		if err != nil {
			errChan <- err
			return
		}
		msgType, err := c.protocol.GetNextMsgType()
		if err != nil {
			errChan <- err
			return
		}
		switch msgType {
		case SimpleStringType:
			res, err := c.protocol.ReadSimpleString()
			if err != nil {
				errChan <- err
				return
			}
			if string(res) != "OK" {
				errChan <- errors.New("unexpected response")
				return
			}
			resChan <- true
		case BulkStringType:
			res, err := c.protocol.ReadBulkString()
			if err != nil {
				errChan <- err
				return
			}
			if res != nil {
				errChan <- errors.New("unexpected response")
				return
			}
			resChan <- false
		case ErrorType:
			resErr, err := c.protocol.ReadError()
			if err != nil {
				errChan <- err
				return
			}
			errChan <- resErr
		default:
			errChan <- errors.New("unexpected response")
			return
		}
	}()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case err := <-errChan:
		return false, err
	case res := <-resChan:
		return res, nil
	}
}

func (c *client) Close() error {
	return c.con.Close()
}
