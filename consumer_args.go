package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// ConsumeArgs specifies the arguments for Consumer.Consume and Consumer.ConsumeEvent.
type ConsumeArgs struct {
	Topics       []string
	RebalanceCb  kafka.RebalanceCb
	Polling      int
	Workers      uint64
	Handler      MessageHandler
	EventHandler EventHandler
}

// SetTopics is a setter.
func (c *ConsumeArgs) SetTopics(topics []string) *ConsumeArgs {
	c.Topics = topics
	return c
}

// SetRebalanceCb is a setter.
func (c *ConsumeArgs) SetRebalanceCb(cb kafka.RebalanceCb) *ConsumeArgs {
	c.RebalanceCb = cb
	return c
}

// SetPolling is a setter.
func (c *ConsumeArgs) SetPolling(polling int) *ConsumeArgs {
	c.Polling = polling
	return c
}

// SetWorkers is a setter.
func (c *ConsumeArgs) SetWorkers(workers uint64) *ConsumeArgs {
	c.Workers = workers
	return c
}

// SetHandler is a setter.
func (c *ConsumeArgs) SetHandler(handler MessageHandler) *ConsumeArgs {
	c.Handler = handler
	return c
}

// SetEventHandler is a setter.
func (c *ConsumeArgs) SetEventHandler(handler EventHandler) *ConsumeArgs {
	c.EventHandler = handler
	return c
}
