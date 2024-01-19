package pkg

import (
	"context"
	"math"
	"time"

	"log"
	"strconv"

	"github.com/pkg/errors"
)

const (
	defalutPoolMaxConns = math.MaxUint
	defaultDailTimeOut  = time.Second
	defaultConIdleTime  = 30 * time.Minute
)

type ClientConfig struct {
	Address string
	// The maximum number of connections in the connection pool. Default is math.MaxUint.
	PoolMaxConns uint
	// The time to connect to the redis server. Default is 1 second.
	DailTimeOut time.Duration
	// The maximum amount of time a connection may be idle. Default is 30 minute.
	ConIdleTime time.Duration
}

func (c *ClientConfig) check() error {
	if c.Address == "" {
		return errors.Wrap(GodisError, "address is empty")
	}
	if c.PoolMaxConns == 0 {
		c.PoolMaxConns = defalutPoolMaxConns
	}
	if c.DailTimeOut == 0 {
		c.DailTimeOut = defaultDailTimeOut
	}
	if c.ConIdleTime == 0 {
		c.ConIdleTime = defaultConIdleTime
	}
	return nil
}

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
	GetEX(ctx context.Context, key string, args ...arg) (*[]byte, error)
	MGet(ctx context.Context, keys ...string) ([]*[]byte, error)
}

type client struct {
	conPool     ConnectionPool
	newProtocol func(Connection) Protocol
	config      *ClientConfig
}

func NewClient(config *ClientConfig) (Client, error) {
	if err := config.check(); err != nil {
		return nil, err
	}
	cp := NewConnectionPool(config.Address, config.PoolMaxConns,
		config.DailTimeOut, config.ConIdleTime)
	return &client{conPool: cp, newProtocol: NewProtocol, config: config}, nil
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
		err1 := c.conPool.Release(con)
		if err1 != nil {
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

func EXArg(seconds uint64) arg {
	return func() []string {
		return []string{"EX", strconv.FormatUint(seconds, 10)}
	}
}

func PXArg(miliseconds uint64) arg {
	return func() []string {
		return []string{"PX", strconv.FormatUint(miliseconds, 10)}
	}
}

func EXATArg(unixTimeSeconds uint64) arg {
	return func() []string {
		return []string{"EXAT", strconv.FormatUint(unixTimeSeconds, 10)}
	}
}

func PXATArg(unixTimeMiliseconds uint64) arg {
	return func() []string {
		return []string{"PXAT", strconv.FormatUint(unixTimeMiliseconds, 10)}
	}
}

var PERSISTArg arg = func() []string {
	return []string{"PERSIST"}
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

func sendReqWithKey(ctx context.Context, protocol Protocol, cmd string, key string, args []arg) error {
	data := [][]byte{
		[]byte(cmd),
		[]byte(key),
	}
	data = append(data, getArgs(args)...)
	return protocol.WriteBulkStringArray(ctx, data)
}

func sendReqWithKeyValue(ctx context.Context, protocol Protocol, cmd string, key string, value []byte, args []arg) error {
	data := [][]byte{
		[]byte(cmd),
		[]byte(key),
		value,
	}
	data = append(data, getArgs(args)...)
	return protocol.WriteBulkStringArray(ctx, data)
}

func sendReqWithKeys(ctx context.Context, protocol Protocol, cmd string, keys []string) error {
	data := [][]byte{
		[]byte(cmd),
	}
	data = append(data, stringsToBytes(keys)...)
	return protocol.WriteBulkStringArray(ctx, data)
}

func readRespStringOrNil(ctx context.Context, protocol Protocol) (interface{}, error) {
	msgType, err := protocol.GetNextMsgType(ctx)
	if err != nil {
		return nil, err
	}
	switch msgType {
	case BulkStringType:
		return protocol.ReadBulkString(ctx)
	case NullType:
		err := protocol.ReadNull(ctx)
		return (*[]byte)(nil), err
	default:
		return (*[]byte)(nil), errors.New("unexpected response")
	}
}
