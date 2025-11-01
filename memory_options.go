package rmq

import (
	"math"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/zohu/zlog"
)

type memoryOptions struct {
	prefix  Prefix
	options *bigcache.Config
}

func (o *memoryOptions) validate() error {
	if o.options == nil {
		o.options = &bigcache.Config{}
	}
	if o.options.Shards <= 0 {
		o.options.Shards = 256
	}
	if o.options.LifeWindow <= 0 {
		o.options.LifeWindow = 10 * time.Minute
	}
	if o.options.MaxEntriesInWindow <= 0 {
		o.options.MaxEntriesInWindow = int(o.options.LifeWindow.Seconds()) * 1000
	}
	if o.options.Logger == nil && o.options.Verbose {
		o.options.Logger = zlog.NewZLogger(&zlog.Options{
			Stack:     zlog.StackOn,
			StackSkip: 1,
		})
	}
	if o.options.CleanWindow <= 0 {
		o.options.CleanWindow = time.Duration(math.Min(float64(o.options.LifeWindow/2), float64(time.Minute)))
	}
	if o.prefix == "" {
		o.prefix = "rmq"
	}
	return nil
}

type MemoryOption func(*memoryOptions)

func WithMemoryOptions(options *bigcache.Config) MemoryOption {
	return func(o *memoryOptions) {
		o.options = options
	}
}
func WithMemoryPrefix(prefix Prefix) MemoryOption {
	return func(o *memoryOptions) {
		o.prefix = prefix
	}
}
