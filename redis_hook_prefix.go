package rmq

import (
	"context"
	"net"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

var prefixableCommands = map[string]bool{
	// Strings
	"GET": true, "SET": true, "EXISTS": true, "TYPE": true,
	"APPEND": true, "DECR": true, "DECRBY": true,
	"GETDEL": true, "GETEX": true,
	"INCR": true, "INCRBY": true, "INCRBYFLOAT": true,
	"STRLEN": true, "GETRANGE": true, "SETRANGE": true,

	// Lists
	"RPUSH": true, "LPUSH": true,
	"LPOP": true, "RPOP": true,
	"BLPOP": true, "BRPOP": true,
	"LINDEX": true, "LINSERT": true, "LLEN": true,
	"LRANGE": true, "LSET": true, "LTRIM": true,
	"LREM": true,

	// Sets
	"SADD": true, "SCARD": true, "SDIFF": true, "SDIFFSTORE": true,
	"SINTER": true, "SINTERSTORE": true, "SISMEMBER": true,
	"SMEMBERS": true, "SPOP": true, "SRANDMEMBER": true,
	"SREM": true, "SUNION": true, "SUNIONSTORE": true,

	// Hashes
	"HDEL": true, "HEXISTS": true, "HGET": true, "HGETALL": true,
	"HINCRBY": true, "HINCRBYFLOAT": true, "HKEYS": true,
	"HLEN": true, "HMGET": true, "HMSET": true, "HSET": true,
	"HSETNX": true, "HSTRLEN": true, "HVALS": true,

	// Sorted Sets
	"ZADD": true, "ZCARD": true, "ZCOUNT": true,
	"ZINCRBY": true, "ZLEXCOUNT": true,
	"ZPOPMAX": true, "ZPOPMIN": true,
	"ZRANGE": true, "ZRANGEBYLEX": true, "ZRANGEBYSCORE": true,
	"ZRANK": true, "ZREM": true, "ZREMRANGEBYLEX": true,
	"ZREMRANGEBYRANK": true, "ZREMRANGEBYSCORE": true,
	"ZREVRANGE": true, "ZREVRANGEBYLEX": true, "ZREVRANGEBYSCORE": true,
	"ZREVRANK": true, "ZSCORE": true,
	"BZPOPMAX": true, "BZPOPMIN": true,
	"ZDIFF": true, "ZDIFFSTORE": true,

	// GEO (built on ZSet)
	"GEOADD": true, "GEOHASH": true, "GEOPOS": true,
	"GEODIST": true, "GEORADIUS": true, "GEORADIUS_RO": true,
	"GEORADIUSBYMEMBER": true, "GEORADIUSBYMEMBER_RO": true,

	// Streams
	"XADD": true, "XLEN": true, "XREAD": true, "XREADGROUP": true,
	"XRANGE": true, "XREVRANGE": true, "XDEL": true, "XTRIM": true,
	"XACK": true, "XCLAIM": true, "XGROUP": true, "XINFO": true,

	// Keys
	"TTL": true, "PTTL": true, "PERSIST": true,
	"DUMP": true, "RESTORE": true,
	"OBJECT": true, // e.g., OBJECT IDLETIME key
	"COPY":   true,

	// Expiry
	"EXPIRE": true, "EXPIREAT": true, "PEXPIRE": true, "PEXPIREAT": true,

	// Others
	"RENAME": true, "RENAMENX": true,
	"PSETEX": true, "GETBIT": true, "SETBIT": true, "BITCOUNT": true, "BITPOS": true, "BITFIELD": true,
	"PFADD": true, "GEOSEARCH": true, "MULTI": true, "EXEC": true,
	"GETSET": true, "SETNX": true, "SETEX": true,
}

func WithSkipPrefix(ctx context.Context) context.Context {
	return context.WithValue(ctx, "rmq_skip", true)
}

func shouldSkipPrefix(ctx context.Context) bool {
	value := ctx.Value("rmq_skip")
	skip, ok := value.(bool)
	return ok && skip
}

type PrefixHook struct {
	prefix Prefix
}

func NewPrefixHook(prefix Prefix) *PrefixHook {
	return &PrefixHook{prefix: prefix}
}
func (h PrefixHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}
func (h PrefixHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !shouldSkipPrefix(ctx) {
			h.addPrefix(cmd)
		}
		return next(ctx, cmd)
	}
}
func (h PrefixHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !shouldSkipPrefix(ctx) {
			for _, cmd := range cmds {
				h.addPrefix(cmd)
			}
		}
		return next(ctx, cmds)
	}
}
func (h PrefixHook) addPrefix(cmd redis.Cmder) {
	args := cmd.Args()
	if len(args) <= 1 {
		return
	}
	name := strings.ToUpper(cmd.Name())
	switch name {
	case "MGET", "DEL", "EXISTS", "TOUCH", "UNLINK", "PFMERGE", "SINTERSTORE",
		"SUNIONSTORE", "SDIFFSTORE", "SDIFF", "SINTER", "SUNION", "PFCOUNT":
		for i := 1; i < len(args); i++ {
			if pattern, ok := toString(args[i]); ok {
				args[i] = h.withPrefix(pattern)
			}
		}
	case "MSET":
		for i := 1; i < len(args); i += 2 {
			if pattern, ok := toString(args[i]); ok {
				args[i] = h.withPrefix(pattern)
			}
		}
	case "BRPOP", "BLPOP", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX": // BRPOP key [key ...] timeout
		for i := 1; i < len(args)-1; i++ {
			if pattern, ok := toString(args[i]); ok {
				args[i] = h.withPrefix(pattern)
			}
		}
	case "XINFO", "XGROUP":
		if len(args) > 2 {
			if pattern, ok := toString(args[2]); ok {
				args[2] = h.withPrefix(pattern)
			}
		}
	case "SCAN":
		if len(args) > 2 {
			for i := 2; i < len(args); i += 2 {
				if s, ok := toString(args[i]); ok && strings.EqualFold(s, "match") && i+1 < len(args) {
					if pattern, ok := toString(args[i+1]); ok {
						args[i+1] = h.withPrefix(pattern)
					}
					break
				}
			}
		}
	case "SSCAN", "ZSCAN":
		if len(args) > 3 {
			if pattern, ok := toString(args[1]); ok {
				args[1] = h.withPrefix(pattern)
			}
			for i := 3; i < len(args); i += 2 {
				if s, ok := toString(args[i]); ok && strings.EqualFold(s, "match") && i+1 < len(args) {
					if pattern, ok := toString(args[i+1]); ok {
						args[i+1] = h.withPrefix(pattern)
					}
					break
				}
			}
		}
	case "HSCAN":
		for i := 2; i < len(args)-1; i++ {
			if s, ok := toString(args[i]); ok && strings.EqualFold(s, "match") {
				if pattern, ok := toString(args[i+1]); ok {
					args[i+1] = h.withPrefix(pattern)
				}
				break
			}
		}
	case "SORT":
		if len(args) > 1 {
			if pattern, ok := toString(args[1]); ok {
				args[1] = h.withPrefix(pattern)
			}
			for i := 2; i < len(args); i++ {
				if s, ok := toString(args[i]); ok {
					if strings.EqualFold(s, "BY") || strings.EqualFold(s, "GET") {
						if i+1 < len(args) {
							if pattern, ok := toString(args[i+1]); ok {
								args[i+1] = h.withPrefix(pattern)
							}
						}
					}
				}
			}
		}
	case "ZDIFF", "ZINTER", "ZUNION":
		if len(args) > 2 {
			if numKeys, ok := toInt(args[1]); ok && numKeys > 0 {
				for i := 2; i < 2+int(numKeys); i++ {
					if pattern, ok := toString(args[i]); ok {
						args[i] = h.withPrefix(pattern)
					}
				}
			}
		}
	case "ZUNIONSTORE", "ZINTERSTORE":
		if len(args) > 1 {
			if pattern, ok := toString(args[1]); ok {
				args[1] = h.withPrefix(pattern)
			}
		}
		if len(args) > 3 {
			if numKeys, ok := toInt(args[1]); ok && numKeys > 0 {
				for i := 3; i < 3+int(numKeys); i++ {
					if pattern, ok := toString(args[i]); ok {
						args[i] = h.withPrefix(pattern)
					}
				}
			}
		}
	case "RPOPLPUSH", "RENAME", "RENAMENX", "COPY", "LMOVE", "BLMOVE", "SMOVE", "GEOSEARCHSTORE":
		if key1, ok := toString(args[1]); ok {
			args[1] = h.withPrefix(key1)
		}
		if key2, ok := toString(args[2]); ok {
			args[2] = h.withPrefix(key2)
		}
	case "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO":
		// 参数结构: [CMD, <script|sha1>, numkeys, key1, key2, ..., arg1, arg2, ...]
		if len(args) < 3 {
			return
		}
		numKeys, ok := toInt(args[2])
		if !ok || numKeys < 0 {
			return
		}
		// KEYS 从 args[3] 开始，共 numKeys 个
		for i := 0; i < numKeys && 3+i < len(args); i++ { // ←←← 起始索引改为 3
			if key, ok := toString(args[3+i]); ok {
				args[3+i] = h.withPrefix(key)
			}
		}
	case "MIGRATE":
		if len(args) > 4 {
			if s, ok := toString(args[3]); ok && s != "" {
				if pattern, ok := toString(args[3]); ok {
					args[3] = h.withPrefix(pattern)
				}
			}
			keysIndex := -1
			for i := 4; i < len(args); i++ {
				if s, ok := toString(args[i]); ok && strings.EqualFold(s, "KEYS") {
					keysIndex = i
					break
				}
			}
			if keysIndex > 0 {
				for i := keysIndex; i < len(args); i++ {
					if pattern, ok := toString(args[i]); ok {
						args[i] = h.withPrefix(pattern)
					}
				}
			}
		}
	case "BITOP":
		// BITOP AND|OR|XOR|NOT destkey srckey1 [srckey2 ...]
		if len(args) < 4 {
			return
		}
		// destkey at args[2]
		if dest, ok := toString(args[2]); ok {
			args[2] = h.withPrefix(dest)
		}
		// source keys from args[3] onward
		for i := 3; i < len(args); i++ {
			if src, ok := toString(args[i]); ok {
				args[i] = h.withPrefix(src)
			}
		}
	case "ZDIFFSTORE":
		// ZDIFFSTORE dest numkeys key [key ...]
		if len(args) < 3 {
			return
		}
		// dest at args[1]
		if dest, ok := toString(args[1]); ok {
			args[1] = h.withPrefix(dest)
		}
		// numkeys at args[2]
		numKeys, ok := toInt(args[2])
		if !ok || numKeys <= 0 {
			return
		}
		// source keys from args[3] to args[3+numKeys-1]
		for i := 0; i < numKeys && 3+i < len(args); i++ {
			if key, ok := toString(args[3+i]); ok {
				args[3+i] = h.withPrefix(key)
			}
		}
	default:
		if h.canWithPrefix(name) {
			if pattern, ok := toString(args[1]); ok {
				args[1] = h.withPrefix(pattern)
			}
		}
	}
}
func (h PrefixHook) canWithPrefix(name string) bool {
	return prefixableCommands[name]
}
func (h PrefixHook) withPrefix(key string) string {
	if strings.HasPrefix(key, h.prefix.String()+":") {
		return key
	}
	return h.prefix.String(key)
}
func toString(v interface{}) (string, bool) {
	if s, ok := v.(string); ok {
		return s, true
	}
	return "", false
}

// 辅助函数：转 int
func toInt(v interface{}) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case string:
		if i, err := strconv.Atoi(x); err == nil {
			return i, true
		}
	}
	return 0, false
}
