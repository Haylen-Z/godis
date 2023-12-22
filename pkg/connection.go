package pkg

import (
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Connection interface {
	io.ReadWriter
	Connect() error
	Close() error
}

var connectTimeOut = 500 * time.Millisecond

type connection struct {
	net.Conn
	address string
}

func (c *connection) Close() error {
	con := c.Conn
	c.Conn = nil
	return con.Close()
}

func (c *connection) Connect() error {
	if c.Conn != nil {
		return nil
	}

	var err error
	c.Conn, err = net.DialTimeout("tcp", c.address, connectTimeOut)
	if err != nil {
		return errors.Wrap(err, "failed to connect to "+c.address)
	}
	return nil
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
	if p.closed {
		return nil, ClosedPoolError
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

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
	if p.closed {
		return ClosedPoolError
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.pool = append(p.pool, conn)
	p.UsedConNum--
	return nil
}

func (p *connectionPool) Close() error {
	if p.closed {
		return nil
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
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
