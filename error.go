package godis

import (
	"errors"
	"fmt"
)

var ErrGodis = errors.New("godis error")
var ErrClosedPool = fmt.Errorf("connection pool is closed: %w", ErrGodis)
var ErrConnectionPoolFull = fmt.Errorf("connection pool is full: %w", ErrGodis)

var errUnexpectedRes = errors.New("unexpected response")
