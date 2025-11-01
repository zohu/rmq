package rmq

import (
	"context"

	"github.com/zohu/zlog"
)

type Monitor struct {
	inner *DelayQueue
}

func NewMonitor(topic Topic, opts ...QueueOption) *Monitor {
	if len(opts) == 0 {
		if o, ok := queueOpts[topic]; ok {
			opts = o
		} else {
			zlog.Panic("rmq: no options provided for topic %s", topic)
		}
	}
	return &Monitor{
		inner: NewQueue(topic, opts...),
	}
}
func (m *Monitor) GetPendingCount() (int64, error) {
	return m.inner.GetPendingCount()
}
func (m *Monitor) GetReadyCount() (int64, error) {
	return m.inner.GetReadyCount()
}
func (m *Monitor) GetProcessingCount() (int64, error) {
	return m.inner.GetProcessingCount()
}
func (m *Monitor) ListenEvent(listener EventListener) (func(), error) {
	ctx := context.Background()
	rds := Rds()
	sub := rds.Subscribe(ctx, m.inner.reportKey)
	cls := func() {
		_ = sub.Close()
	}
	resultChan := make(chan string)
	go func() {
		for msg := range sub.Channel() {
			resultChan <- msg.Payload
		}
	}()
	go func() {
		for payload := range resultChan {
			event, err := decodeEvent(payload)
			if err != nil {
				zlog.Warnf("[listen event] %v", event)
			} else {
				listener.OnEvent(event)
			}
		}
	}()
	return cls, nil
}
