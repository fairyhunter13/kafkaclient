package kafkaclient

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/panjf2000/ants/v2"
)

// Consumer promotes the origin consumer of kafka client.
type Consumer struct {
	*kafka.Consumer
}

// EventHandler is a handler for handling event.
type EventHandler func(cons *Consumer, event kafka.Event)

// MessageHandler is a handler for handling message.
type MessageHandler func(cons *Consumer, msg *kafka.Message, err error)

// GetOrigin returns the origin consumer of kafka.
func (c *Consumer) GetOrigin() *kafka.Consumer {
	return c.Consumer
}

// Consume consumes the messages from the kafka broker using MessageHandler.
func (c *Consumer) Consume(args ConsumeArgs) (err error) {
	args.fixArgs()
	err = c.SubscribeTopics(args.Topics, args.RebalanceCb)
	if err != nil {
		return
	}

	for numWorker := uint64(1); numWorker <= args.Workers; numWorker++ {
		ants.Submit(func() {
			var (
				err error
				msg *kafka.Message
			)
			for {
				msg, err = c.ReadMessage(time.Duration(args.Polling) * time.Millisecond)
				args.Handler(c, msg, err)
			}
		})
	}

	return
}

// ConsumeEvent consumes the events from the kafka broker using EventHandler.
func (c *Consumer) ConsumeEvent(args ConsumeArgs) (err error) {
	args.fixArgs()
	err = c.SubscribeTopics(args.Topics, args.RebalanceCb)
	if err != nil {
		return
	}

	for numWorker := uint64(1); numWorker <= args.Workers; numWorker++ {
		ants.Submit(func() {
			var (
				event kafka.Event
			)
			for {
				event = c.Poll(args.Polling)
				args.EventHandler(c, event)
			}
		})
	}
	return
}
