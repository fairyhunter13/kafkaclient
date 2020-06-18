package kafkaclient

import "github.com/confluentinc/confluent-kafka-go/kafka"

// Producer promotes the producer of kafka client.
type Producer struct {
	*kafka.Producer
}

// GetOrigin returns the producer origin from kafka.
func (p *Producer) GetOrigin() *kafka.Producer {
	return p.Producer
}

// Close close the underlying resources.
func (p *Producer) Close() (err error) {
	p.Producer.Close()
	return
}
