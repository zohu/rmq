package rmq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type DictName Prefix

func (t DictName) String(args ...string) string {
	return Prefix(t).String(args...)
}

type DictQuery func(ctx context.Context, key string) string
type dict struct {
	query DictQuery
	ttl   time.Duration
}

var dicts = sync.Map{}

func NewDict(name DictName, query DictQuery, ttl ...time.Duration) {
	ttl = append(ttl, time.Hour*24)
	dicts.Store(name, &dict{
		query: query,
		ttl:   ttl[0],
	})
}
func Dict(ctx context.Context, name DictName, key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("key is empty")
	}
	if v, err := CacheL2().Get(ctx, name.String(key)); err == nil {
		return v, nil
	}
	if opt, ok := dicts.Load(name); ok {
		d := opt.(*dict)
		if value := d.query(ctx, key); value != "" {
			CacheL2().Set(ctx, name.String(key), value, d.ttl)
			return value, nil
		}
	} else {
		return "", fmt.Errorf("dict %s not registered, call NewDict first", name)
	}
	return "", fmt.Errorf("not found: %s", key)
}
