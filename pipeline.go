package godis

import "context"

type Pipeline struct {
	client   *client
	commands []Command
}

func (p *Pipeline) Exec(ctx context.Context) ([]interface{}, error) {
	r, err := p.client.exec(ctx, p)
	if err != nil {
		return nil, err
	}
	return r.([]interface{}), nil
}

func (p *Pipeline) SendReq(ctx context.Context, protocol Protocol) error {
	for _, cmd := range p.commands {
		err := cmd.SendReq(ctx, protocol)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) ReadResp(ctx context.Context, protocol Protocol) (interface{}, error) {
	var res []interface{}
	for _, cmd := range p.commands {
		r, err := cmd.ReadResp(ctx, protocol)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (c *client) Pipeline() *Pipeline {
	return &Pipeline{client: c}
}

// String commands

func (p *Pipeline) Append(key string, value string) {
	p.commands = append(p.commands, &stringAppendCommand{key: key, value: value})
}

func (p *Pipeline) Decr(key string) {
	p.commands = append(p.commands, &stringDecrCommand{key: key})
}

func (p *Pipeline) DecrBy(key string, decrement int64) {
	p.commands = append(p.commands, &stringDecrByCommand{key: key, decrement: decrement})
}

func (p *Pipeline) Get(key string) {
	p.commands = append(p.commands, &stringGetCommand{key: key})
}

func (p *Pipeline) GetDel(key string) {
	p.commands = append(p.commands, &stringGetDelCommand{key: key})
}

func (p *Pipeline) GetEX(key string, optArgs ...arg) {
	p.commands = append(p.commands, &stringGetEXCommand{key: key})
}

func (p *Pipeline) Lcs(key1 string, key2 string, args ...arg) {
	p.commands = append(p.commands, &stringLcsCommand{key1: key1, key2: key2, args: args})
}

func (p *Pipeline) LcsLen(key1 string, key2 string) {
	p.commands = append(p.commands, &stringLcsLenCommand{key1: key1, key2: key2})
}

func (p *Pipeline) LcsIdx(key1 string, key2 string, args ...arg) {
	p.commands = append(p.commands, &stringLcsIdxCommand{key1: key1, key2: key2, args: args})
}

func (p *Pipeline) LcsIdxWithMatchLen(key1 string, key2 string, args ...arg) {
	p.commands = append(p.commands, &stringLcsIdxWithMatchLenCommand{key1: key1, key2: key2, args: args})
}

func (p *Pipeline) GetRange(key string, start int64, end int64) {
	p.commands = append(p.commands, &stringGetRangeCommand{key: key, start: start, end: end})
}

func (p *Pipeline) GetSet(key string, value string) {
	p.commands = append(p.commands, &stringGetSetCommand{key: key, value: value})
}

func (p *Pipeline) Incr(key string) {
	p.commands = append(p.commands, &stringIncrCommand{key: key})
}

func (p *Pipeline) IncrBy(key string, increment int64) {
	p.commands = append(p.commands, &stringIncrByCommand{key: key, increment: increment})
}

func (p *Pipeline) IncrByFloat(key string, increment float64) {
	p.commands = append(p.commands, &stringIncrByFloatCommand{key: key, increment: increment})
}

func (p *Pipeline) MGet(keys ...string) {
	p.commands = append(p.commands, &stringMGetCommand{keys: keys})
}

func (p *Pipeline) MSet(kvs map[string]string) {
	p.commands = append(p.commands, &stringMSetCommand{kvs: kvs})
}

func (p *Pipeline) MSetNX(kvs map[string]string) {
	p.commands = append(p.commands, &stringMSetNxCommand{kvs: kvs})
}

func (p *Pipeline) PSetEX(key, value string, milliseconds uint64) {
	p.commands = append(p.commands, &stringPSetEXCommand{key: key, value: value, milliseconds: milliseconds})
}

func (p *Pipeline) Set(key string, value string, args ...arg) {
	p.commands = append(p.commands, &stringSetCommand{key: key, value: value, args: args})
}

func (p *Pipeline) SetEX(key, value string, seconds uint64) {
	p.commands = append(p.commands, &stringSetEXCommand{key: key, value: value, seconds: seconds})
}

func (p *Pipeline) SetNX(key, value string) {
	p.commands = append(p.commands, &stringSetNXCommand{key: key, value: value})
}

func (p *Pipeline) SetRange(key string, offset uint, value string) {
	p.commands = append(p.commands, &stringSetRangeCommand{key: key, value: value, offset: offset})
}

func (p *Pipeline) StrLen(key string) {
	p.commands = append(p.commands, &stringStrLenCommand{key: key})
}

func (p *Pipeline) SubStr(key string, start, end int) {
	p.commands = append(p.commands, &stringSubStrCommand{key: key, start: start, end: end})
}
