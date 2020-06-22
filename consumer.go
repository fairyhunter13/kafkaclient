package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer promotes the origin consumer of kafka client.
type Consumer struct {
	*kafka.Consumer
}

// EventHandler is a handler for handling event.
type EventHandler func(cons *Consumer, event kafka.Event)

// MessageHandler is a handler for handling message.
type MessageHandler func(cons *Consumer, msg *kafka.Message)

// GetOrigin returns the origin consumer of kafka.
func (c *Consumer) GetOrigin() *kafka.Consumer {
	return c.Consumer
}

func (c *Consumer) consume(args ConsumeArgs) (err error) {
	err = c.SubscribeTopics(args.Topics, args.RebalanceCb)
	if err != nil {
		return
	}

	go func(c *Consumer, args ConsumeArgs) {
		for {
			event := c.Poll(args.Polling)
			switch realType := event.(type) {
			case *kafka.Message:
				args.Handler(c, realType)
			}
		}
	}(c, args)

	return
}

func (c *Consumer) consumeEvent(args ConsumeArgs) (err error) {
	err = c.SubscribeTopics(args.Topics, args.RebalanceCb)
	if err != nil {
		return
	}

	go func(c *Consumer, args ConsumeArgs) {
		for {
			event := c.Poll(args.Polling)
			args.EventHandler(c, event)
		}
	}(c, args)
	return
}

func (c *Consumer) consumeBatch(args ConsumeArgs) (err error) {
	err = c.SubscribeTopics(args.Topics, args.RebalanceCb)
	if err != nil {
		return
	}

	go func(c *Consumer, args ConsumeArgs) {
		for event := range c.Events() {
			switch realType := event.(type) {
			case *kafka.Message:
				args.Handler(c, realType)
			}
		}
	}(c, args)
	return
}

func (c *Consumer) consumeEventBatch(args ConsumeArgs) (err error) {
	err = c.SubscribeTopics(args.Topics, args.RebalanceCb)
	if err != nil {
		return
	}

	go func(c *Consumer, args ConsumeArgs) {
		for event := range c.Events() {
			args.EventHandler(c, event)
		}
	}(c, args)
	return
}
