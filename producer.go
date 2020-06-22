package kafkaclient

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer promotes the producer of kafka client.
type Producer struct {
	*kafka.Producer
}

// TimeoutFlush specifies the flush's timeout for the producer.
var TimeoutFlush = 5 * time.Second

// GetOrigin returns the producer origin from kafka.
func (p *Producer) GetOrigin() *kafka.Producer {
	return p.Producer
}

// Close close the underlying resources.
func (p *Producer) Close() (err error) {
	p.Producer.Flush(int(TimeoutFlush.Milliseconds()))
	p.Producer.Close()
	return
}

// Publish publishes message synchronously to the kafka's brokers.
func (p *Producer) Publish(msg *kafka.Message) (event kafka.Event, err error) {
	eventChan := make(chan kafka.Event, 1)
	defer close(eventChan)
	err = p.Produce(msg, eventChan)
	if err != nil {
		return
	}
	event = <-eventChan
	return
}

// PublishAsync publishes message asynchronously to the kafka's brokers.
func (p *Producer) PublishAsync(msg *kafka.Message, eventChan chan kafka.Event) (err error) {
	err = p.Produce(msg, eventChan)
	return
}
