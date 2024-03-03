package godis

import (
	"context"
	"strconv"
)

func DBArg(db uint) arg {
	return func() []string {
		return []string{strconv.FormatUint(uint64(db), 10)}
	}
}

var REPLACEArg arg = func() []string {
	return []string{"REPLACE"}
}

type copyCommand struct {
	source string
	dest   string
	args   []arg
}

func (c *copyCommand) SendReq(ctx context.Context, protocol Protocol) error {
	return sendReq(ctx, protocol, []string{"COPY", c.source, c.dest}, c.args)
}

func (c *copyCommand) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	r, err := protocol.ReadInteger(ctx)
	if err != nil {
		return false, err
	}
	return r == 1, err
}

func (c *client) Copy(ctx context.Context, source, dest string, args ...arg) (bool, error) {
	cmd := &copyCommand{source: source, dest: dest, args: args}
	res, err := c.exec(ctx, cmd)
	if err != nil {
		return false, err
	}
	return res.(bool), err
}
