package kafkaclient

import "github.com/confluentinc/confluent-kafka-go/kafka"

func (c *Container) initConfig(config kafka.ConfigMap) (newConfig kafka.ConfigMap) {
	if config != nil {
		newConfig = c.cloneConfig(config)
		c.appendConfigMap(c.configMap, newConfig)
	} else {
		newConfig = c.cloneConfig(c.configMap)
	}
	return
}

func (c *Container) appendConfigMap(oldConfig kafka.ConfigMap, newConfig kafka.ConfigMap) {
	for key, value := range oldConfig {
		newConfig[key] = value
	}
	return
}

func (c *Container) cloneConfig(config kafka.ConfigMap) (newConfig kafka.ConfigMap) {
	newConfig = kafka.ConfigMap{}
	if config == nil {
		return
	}
	for key, value := range config {
		newConfig[key] = value
	}
	return
}
