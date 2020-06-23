package kafkaclient

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Container contains all variables and configs to run kafka.
type Container struct {
	configMap        kafka.ConfigMap
	closeSignal      chan bool
	resourcesCounter uint64
}

// NewContainer initialize a new container.
func NewContainer(config kafka.ConfigMap) *Container {
	if config == nil {
		config = kafka.ConfigMap{}
	}
	return &Container{
		configMap:   config,
		closeSignal: make(chan bool),
	}
}

// NewProducer initialize a new producer from this container.
func (c *Container) NewProducer(config kafka.ConfigMap) (prod *Producer, err error) {
	newConfig := c.initConfig(config)
	c.assignClientID(TypeProducer, newConfig)
	originProducer, err := kafka.NewProducer(&newConfig)
	if err != nil {
		return
	}
	prod = &Producer{originProducer}
	c.close(prod)
	return
}

// NewAdminClient initialize a new admin client from this container.
func (c *Container) NewAdminClient(config kafka.ConfigMap) (ac *AdminClient, err error) {
	newConfig := c.initConfig(config)
	c.assignClientID(TypeAdminClient, newConfig)
	originAC, err := kafka.NewAdminClient(&newConfig)
	if err != nil {
		return
	}
	ac = &AdminClient{originAC}
	c.close(ac)
	return
}

// NewConsumer initialize a new consumer from this container.
func (c *Container) NewConsumer(config kafka.ConfigMap) (cons *Consumer, err error) {
	newConfig := c.initConfig(config)
	c.assignClientID(TypeConsumer, newConfig)
	originCons, err := kafka.NewConsumer(&newConfig)
	if err != nil {
		return
	}
	cons = &Consumer{originCons}
	c.close(cons)
	return
}

// GetClientID returns unique client id based on the time.
func (c *Container) GetClientID(resource string) string {
	return fmt.Sprintf("%s-%s", resource, time.Now().Add(1*time.Nanosecond).Format(time.RFC3339Nano))
}

func (c *Container) assignClientID(resource string, config kafka.ConfigMap) {
	config[ClientID] = c.GetClientID(resource)
}
