package pkg

import (
	"context"
	"math"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Connection interface {
	Read(ctx context.Context, p []byte) (n int, err error)
	Write(ctx context.Context, p []byte) (n int, err error)
	Connect() error
	Close() error
}

var connectTimeOut = 500 * time.Millisecond

type connection struct {
	con     net.Conn
	address string
}

func (c *connection) Close() error {
	con := c.con
	c.con = nil
	return con.Close()
}

func (c *connection) Connect() error {
	if c.con != nil {
		return nil
	}

	var err error
	c.con, err = net.DialTimeout("tcp", c.address, connectTimeOut)
	if err != nil {
		return errors.Wrap(err, "failed to connect to "+c.address)
	}
	return nil
}

func (c *connection) Read(ctx context.Context, p []byte) (n int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if dl, ok := ctx.Deadline(); ok {
		if err := c.con.SetReadDeadline(dl); err != nil {
			return 0, err
		}
	}
	return c.con.Read(p)
}

func (c *connection) Write(ctx context.Context, p []byte) (n int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if dl, ok := ctx.Deadline(); ok {
		if err := c.con.SetWriteDeadline(dl); err != nil {
			return 0, err
		}
	}
	return c.con.Write(p)
}

func NewConnection(address string) Connection {
	return &connection{address: address}
}

type ConnectionPool interface {
	GetConnection() (Connection, error)
	Release(Connection) error
	Close() error
}

var ClosedPoolError = errors.New("connection pool is closed")
var ConnectionPoolFullError = errors.New("connection pool is full")

const DefaultMaxConNum = math.MaxInt32

type connectionPool struct {
	address       string
	UsedConNum    int
	AllConNum     int
	MaxConNum     int
	pool          []Connection
	newConnection func(address string) Connection
	mutex         *sync.Mutex
	closed        bool
}

func NewConnectionPool(address string, maxConNum int) ConnectionPool {
	return &connectionPool{mutex: &sync.Mutex{}, address: address, MaxConNum: maxConNum,
		newConnection: NewConnection}
}

func (p *connectionPool) GetConnection() (Connection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil, ClosedPoolError
	}

	if len(p.pool) == 0 {
		if p.AllConNum >= p.MaxConNum {
			return nil, ConnectionPoolFullError
		}
		conn := p.newConnection(p.address)
		if err := conn.Connect(); err != nil {
			return nil, err
		}
		p.AllConNum++
		p.UsedConNum++
		return conn, nil
	}

	// TODO: health check
	con := p.pool[len(p.pool)-1]
	p.pool = p.pool[:len(p.pool)-1]
	p.UsedConNum++
	return con, nil
}

func (p *connectionPool) Release(conn Connection) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return ClosedPoolError
	}

	p.pool = append(p.pool, conn)
	p.UsedConNum--
	return nil
}

func (p *connectionPool) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil
	}

	for _, conn := range p.pool {
		err := conn.Close()
		if err != nil {
			return err
		}
	}
	p.pool = nil
	p.closed = true
	p.AllConNum = 0
	p.UsedConNum = 0
	return nil
}
