package kafkaclient

import "github.com/confluentinc/confluent-kafka-go/kafka"

// Consumer promotes the origin consumer of kafka client.
type Consumer struct {
	*kafka.Consumer
}

// GetOrigin returns the origin consumer of kafka.
func (c *Consumer) GetOrigin() *kafka.Consumer {
	return c.Consumer
}
