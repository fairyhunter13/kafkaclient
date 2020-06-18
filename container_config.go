package kafkaclient

import "github.com/confluentinc/confluent-kafka-go/kafka"

func (c *Container) initConfig(config kafka.ConfigMap) (newConfig kafka.ConfigMap) {
	if config != nil {
		newConfig = kafka.ConfigMap{}
		c.appendConfigMap(c.configMap, newConfig)
		c.appendConfigMap(config, newConfig)
	} else {
		newConfig = c.configMap
	}
	return
}

func (c *Container) appendConfigMap(oldConfig kafka.ConfigMap, newConfig kafka.ConfigMap) {
	for key, value := range oldConfig {
		newConfig[key] = value
	}
	return
}
