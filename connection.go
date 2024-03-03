package godis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Connection interface {
	Read(ctx context.Context, p []byte) (n int, err error)
	Write(ctx context.Context, p []byte) (n int, err error)
	GetLastUsedAt() time.Time
	IsBroken() bool
	SetBroken()
	Connect() error
	Close() error
}

type ConnectionConfig struct {
	Address     string
	DialTimeOut time.Duration

	Tls           bool
	TlsCertPath   string
	TlsCaCertPath string
	TlsKeyPath    string
}

type connection struct {
	con        net.Conn
	lastUsedAt time.Time
	broken     bool
	config     *ConnectionConfig
}

func (c *connection) IsBroken() bool {
	return c.broken
}

func (c *connection) SetBroken() {
	c.broken = true
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
	if c.config.Tls {
		c.con, err = c.dialTls()
	} else {
		c.con, err = net.DialTimeout("tcp", c.config.Address, c.config.DialTimeOut)
	}
	if err != nil {
		return errors.Wrap(err, "failed to connect to "+c.config.Address)
	}
	c.lastUsedAt = time.Now()
	return nil
}

func (c *connection) dialTls() (net.Conn, error) {
	cert, err := tls.LoadX509KeyPair(c.config.TlsCertPath, c.config.TlsKeyPath)

	if err != nil {
		return nil, errors.Wrap(err, "failed to load cert")
	}

	pem, err := os.ReadFile(c.config.TlsCaCertPath)
	if err != nil {
		return nil, errors.New("failed to load ca")
	}
	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(pem); !ok {
		return nil, errors.New("failed to load ca")
	}

	return tls.Dial("tcp", c.config.Address, &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
	})
}

func (c *connection) Read(ctx context.Context, p []byte) (n int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if dl, ok := ctx.Deadline(); ok {
		if err := c.con.SetReadDeadline(dl); err != nil {
			return 0, errors.Wrap(err, "failed to set read deadline")
		}
	}
	n, err = c.con.Read(p)
	if err != nil {
		return n, errors.Wrap(err, "failed to read from connection")
	}
	c.lastUsedAt = time.Now()
	return
}

func (c *connection) Write(ctx context.Context, p []byte) (n int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if dl, ok := ctx.Deadline(); ok {
		if err := c.con.SetWriteDeadline(dl); err != nil {
			return 0, errors.Wrap(err, "failed to set write deadline")
		}
	}
	n, err = c.con.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "failed to write to connection")
	}
	c.lastUsedAt = time.Now()
	return
}

func NewConnection(config *ConnectionConfig) Connection {
	return &connection{config: config}
}

type ConnectionPool interface {
	GetConnection() (Connection, error)
	Release(Connection) error
	Close() error
}

type ConnectionPoolConfig struct {
	ConnectionConfig
	ConIdleTime    time.Duration
	ConDialTimeOut time.Duration
	MaxIdleConNum  uint
	MaxConNum      uint
}

type connectionPool struct {
	UsedConNum    uint
	AllConNum     uint
	pool          []Connection
	newConnection func(*ConnectionConfig) Connection
	mutex         *sync.Mutex
	closed        bool
	config        *ConnectionPoolConfig
	conCloseChan  chan Connection
}

func NewConnectionPool(config *ConnectionPoolConfig) ConnectionPool {
	p := &connectionPool{mutex: &sync.Mutex{},
		newConnection: NewConnection, conCloseChan: make(chan Connection),
		config: config,
	}
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

func (p *connectionPool) popCon() Connection {
	con := p.pool[len(p.pool)-1]
	p.pool[len(p.pool)-1] = nil
	p.pool = p.pool[:len(p.pool)-1]
	return con
}

func (p *connectionPool) tryGetHealthConn() Connection {
	for len(p.pool) > 0 {
		con := p.popCon()
		if time.Since(con.GetLastUsedAt()) > p.config.ConIdleTime {
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
		return nil, ErrClosedPool
	}

	con := p.tryGetHealthConn()
	if con == nil {
		if p.AllConNum >= p.config.MaxConNum {
			return nil, ErrConnectionPoolFull
		}
		con = p.newConnection(&p.config.ConnectionConfig)
		if err := con.Connect(); err != nil {
			return nil, err
		}
		p.AllConNum++

	}
	p.clearIdleCon()
	p.UsedConNum++
	return con, nil
}

func (p *connectionPool) clearIdleCon() {
	if p.config.MaxIdleConNum != 0 && len(p.pool) > int(p.config.MaxIdleConNum) {
		con := p.popCon()
		p.AllConNum--
		p.conCloseChan <- con
	}
}

func (p *connectionPool) Release(conn Connection) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return ErrClosedPool
	}

	p.UsedConNum--
	if conn.IsBroken() {
		p.AllConNum--
		p.conCloseChan <- conn
		return nil
	}
	p.pool = append(p.pool, conn)
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
