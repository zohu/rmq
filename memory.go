package rmq

import (
	"context"
	"strings"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/bytedance/sonic"
	"github.com/zohu/zlog"
)

type Memory struct {
	prefix Prefix
	life   time.Duration
	cache  *bigcache.BigCache
}

func NewMemory(opts ...MemoryOption) *Memory {
	option := &memoryOptions{}
	for _, opt := range opts {
		opt(option)
	}
	if err := option.validate(); err != nil {
		zlog.Panic("rmq: memory options error", "err", err)
	}
	c, err := bigcache.New(context.Background(), *option.options)
	if err != nil {
		zlog.Panic("rmq: memory options error", "err", err)
	}
	return &Memory{
		prefix: option.prefix,
		life:   option.options.LifeWindow,
		cache:  c,
	}
}

type MemEntry struct {
	Expire int64  `json:"e"`
	Value  string `json:"v"`
}

func (m *Memory) Set(key string, value string, ttl ...time.Duration) {
	ttl = append(ttl, m.life)
	expire := time.Now().Add(ttl[0]).UnixNano()
	data, _ := sonic.Marshal(MemEntry{Expire: expire, Value: value})
	_ = m.cache.Set(m.appendPrefix(key), data)
}
func (m *Memory) Get(key string) (string, bool) {
	d, err := m.cache.Get(m.appendPrefix(key))
	if err != nil {
		return "", false
	}
	var obj MemEntry
	if err = sonic.Unmarshal(d, &obj); err != nil {
		return "", false
	}
	if obj.Expire < time.Now().UnixNano() {
		_ = m.cache.Delete(m.appendPrefix(key))
		return "", false
	}
	return obj.Value, true
}
func (m *Memory) Delete(key string) {
	_ = m.cache.Delete(m.appendPrefix(key))
}
func (m *Memory) Close() error {
	return m.cache.Close()
}
func (m *Memory) Len() int {
	return m.cache.Len()
}
func (m *Memory) Capacity() int {
	return m.cache.Capacity()
}
func (m *Memory) Range(fn func(key string, value string)) {
	iter := m.cache.Iterator()
	for iter.SetNext() {
		v, err := iter.Value()
		if err != nil {
			continue
		}
		var obj MemEntry
		if err = sonic.Unmarshal(v.Value(), &obj); err != nil {
			continue
		}
		if obj.Expire < time.Now().UnixNano() {
			_ = m.cache.Delete(v.Key())
			continue
		}
		fn(m.removePrefix(v.Key()), obj.Value)
	}
}
func (m *Memory) Stats() bigcache.Stats {
	return m.cache.Stats()
}
func (m *Memory) KeyMetadata(key string) bigcache.Metadata {
	return m.cache.KeyMetadata(m.appendPrefix(key))
}
func (m *Memory) appendPrefix(k string) string {
	if m.prefix == "" || strings.HasPrefix(k, m.prefix.String()+":") {
		return k
	}
	return m.prefix.String(k)
}
func (m *Memory) removePrefix(k string) string {
	if m.prefix == "" || !strings.HasPrefix(k, m.prefix.String()+":") {
		return k
	}
	return k[len(m.prefix+":"):]
}
