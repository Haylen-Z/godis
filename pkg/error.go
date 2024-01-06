package pkg

import (
	"github.com/pkg/errors"
)

var GodisError = errors.New("godis error")
var ClosedPoolError = errors.Wrap(GodisError, "connection pool is closed")
var ConnectionPoolFullError = errors.Wrap(GodisError, "connection pool is full")
