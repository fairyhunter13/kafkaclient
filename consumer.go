package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer promotes the origin consumer of kafka client.
type Consumer struct {
	*kafka.Consumer
}

// EventHandler is a handler for handling event.
// It will automatically retry the handler for the current message if it returns error.
type EventHandler func(cons *Consumer, event kafka.Event) (err error)

// MessageHandler is a handler for handling message.
// It will automatically retry the handler for the current event if it returns error.
type MessageHandler func(cons *Consumer, msg *kafka.Message) (err error)

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
				c.handleMessage(realType, &args)
			case kafka.AssignedPartitions:
				c.Assign(realType.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
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
			c.handleEvent(event, &args)
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
				c.handleMessage(realType, &args)
			case kafka.AssignedPartitions:
				c.Assign(realType.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
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
			c.handleEvent(event, &args)
		}
	}(c, args)
	return
}

func (c *Consumer) handleMessage(msg *kafka.Message, args *ConsumeArgs) {
	err := args.Handler(c, msg)
	for err != nil {
		err = args.Handler(c, msg)
	}
}

func (c *Consumer) handleEvent(event kafka.Event, args *ConsumeArgs) {
	err := args.EventHandler(c, event)
	for err != nil {
		err = args.EventHandler(c, event)
	}
}
