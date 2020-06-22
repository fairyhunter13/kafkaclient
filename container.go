package kafkaclient

import (
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
	originCons, err := kafka.NewConsumer(&newConfig)
	if err != nil {
		return
	}
	cons = &Consumer{originCons}
	c.close(cons)
	return
}
