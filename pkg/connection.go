package pkg

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Connection interface {
	Read(ctx context.Context, p []byte) (n int, err error)
	Write(ctx context.Context, p []byte) (n int, err error)
	GetLastUsedAt() time.Time
	Connect() error
	Close() error
}

var connectTimeOut = 500 * time.Millisecond

type connection struct {
	con        net.Conn
	address    string
	lastUsedAt time.Time
}

func (c *connection) GetLastUsedAt() time.Time {
	return c.lastUsedAt
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
	c.lastUsedAt = time.Now()
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
	n, err = c.con.Read(p)
	c.lastUsedAt = time.Now()
	return
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
	n, err = c.con.Write(p)
	c.lastUsedAt = time.Now()
	return
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

var defaultConIdleTime = 30 * time.Minute

type connectionPool struct {
	address       string
	UsedConNum    int
	AllConNum     int
	MaxConNum     int
	pool          []Connection
	newConnection func(address string) Connection
	mutex         *sync.Mutex
	closed        bool
	conIdleTime   time.Duration
	conCloseChan  chan Connection
}

func NewConnectionPool(address string, maxConNum int) ConnectionPool {
	p := &connectionPool{mutex: &sync.Mutex{}, address: address, MaxConNum: maxConNum,
		newConnection: NewConnection, conIdleTime: defaultConIdleTime, conCloseChan: make(chan Connection)}
	p.startCloseConWorker()
	return p

}

func (p *connectionPool) startCloseConWorker() {
	go func() {
		for con := range p.conCloseChan {
			if err := con.Close(); err != nil {
				log.Println("failed to close connection: ", err)
			}
		}
	}()
}

func (p *connectionPool) tryGetHealthConn() Connection {
	for len(p.pool) > 0 {
		con := p.pool[len(p.pool)-1]
		p.pool = p.pool[:len(p.pool)-1]
		p.UsedConNum++
		if time.Since(con.GetLastUsedAt()) > p.conIdleTime {
			p.AllConNum--
			p.conCloseChan <- con
			continue
		}
		return con
	}
	return nil
}

func (p *connectionPool) GetConnection() (Connection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil, ClosedPoolError
	}

	con := p.tryGetHealthConn()
	if con == nil {
		if p.AllConNum >= p.MaxConNum {
			return nil, ConnectionPoolFullError
		}
		con = p.newConnection(p.address)
		if err := con.Connect(); err != nil {
			return nil, err
		}
		p.AllConNum++
		p.UsedConNum++
	}
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
		p.conCloseChan <- conn
	}
	close(p.conCloseChan)
	p.pool = nil
	p.closed = true
	p.AllConNum = 0
	p.UsedConNum = 0
	return nil
}
