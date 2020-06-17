package kafkaclient

import "github.com/confluentinc/confluent-kafka-go/kafka"

// AdminClient promotes the origin admin client of kafka client.
type AdminClient struct {
	*kafka.AdminClient
}

// GetOrigin returns the origin admin client from kafka.
func (ac *AdminClient) GetOrigin() *kafka.AdminClient {
	return ac.AdminClient
}
