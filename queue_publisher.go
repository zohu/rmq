package rmq

import (
	"time"

	"github.com/zohu/zlog"
)

type Publisher struct {
	inner *DelayQueue
}

func NewPublisher(topic Topic, opts ...QueueOption) *Publisher {
	if len(opts) == 0 {
		if o, ok := queueOpts[topic]; ok {
			opts = o
		} else {
			zlog.Panic("rmq: no options provided for topic %s", topic)
		}
	}
	return &Publisher{
		inner: NewQueue(topic, opts...),
	}
}
func (p *Publisher) SendScheduleMsg(payload string, t time.Time) (string, error) {
	return p.inner.SendScheduleMsg(payload, t)
}
func (p *Publisher) SendDelayMsg(payload string, duration time.Duration) (string, error) {
	return p.inner.SendDelayMsg(payload, duration)
}
