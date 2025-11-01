package rmq

import (
	"math"
	"time"
)

type queueOptions struct {
	prefix              Prefix
	hashTag             *bool
	fetchLimit          uint
	fetchInterval       time.Duration
	concurrent          uint
	retryCount          uint
	maxConsumeDuration  time.Duration
	nackRedeliveryDelay time.Duration
	ttl                 time.Duration
}

func (o *queueOptions) validate() error {
	if o.hashTag == nil {
		o.hashTag = Ptr(true)
	}
	o.concurrent = uint(math.Max(float64(o.concurrent), 1))
	if o.fetchInterval <= 0 {
		o.fetchInterval = time.Second
	}
	if o.retryCount <= 0 {
		o.retryCount = 3
	}
	if o.maxConsumeDuration <= 0 {
		o.maxConsumeDuration = 5 * time.Second
	}
	if o.ttl <= 0 {
		o.ttl = time.Hour * 24 * 30
	}
	if o.prefix == "" {
		o.prefix = "rmq"
	}
	return nil
}

type QueueOption func(*queueOptions)

func WithQueuePrefix(prefix string) QueueOption {
	return func(o *queueOptions) {
		o.prefix = Prefix(prefix)
	}
}
func WithQueueConcurrent(concurrent uint) QueueOption {
	return func(o *queueOptions) {
		o.concurrent = concurrent
	}
}
func WithQueueFetchInterval(interval time.Duration) QueueOption {
	return func(o *queueOptions) {
		o.fetchInterval = interval
	}
}
func WithQueueFetchLimit(limit uint) QueueOption {
	return func(o *queueOptions) {
		o.fetchLimit = limit
	}
}
func WithQueueHashTag(hashTag bool) QueueOption {
	return func(o *queueOptions) {
		o.hashTag = &hashTag
	}
}
func WithQueueMaxConsumeDuration(duration time.Duration) QueueOption {
	return func(o *queueOptions) {
		o.maxConsumeDuration = duration
	}
}
func WithQueueDefaultTTL(ttl time.Duration) QueueOption {
	return func(o *queueOptions) {
		o.ttl = ttl
	}
}
func WithQueueNackRedeliveryDelay(delay time.Duration) QueueOption {
	return func(o *queueOptions) {
		o.nackRedeliveryDelay = delay
	}
}
func WithQueueRetryCount(count uint) QueueOption {
	return func(o *queueOptions) {
		o.retryCount = count
	}
}
