package kafkaclient

import "github.com/confluentinc/confluent-kafka-go/kafka"

// Consume create consumers based on per thread and directly consume messages from the Kafka broker.
func (c *Container) Consume(config kafka.ConfigMap, args ConsumeArgs) (consList []*Consumer, err error) {
	newConfig := c.cloneConfig(config)
	newConfig[GoApplicationRebalanceEnable] = true
	for numWorker := uint64(1); numWorker <= args.Workers; numWorker++ {
		var cons *Consumer
		cons, err = c.NewConsumer(newConfig)
		if err != nil {
			return
		}
		err = cons.consume(args)
		if err != nil {
			return
		}
		consList = append(consList, cons)
	}
	return
}

// ConsumeEvent create consumers based on per thread and directly consume events from the Kafka broker.
func (c *Container) ConsumeEvent(config kafka.ConfigMap, args ConsumeArgs) (consList []*Consumer, err error) {
	for numWorker := uint64(1); numWorker <= args.Workers; numWorker++ {
		var cons *Consumer
		cons, err = c.NewConsumer(config)
		if err != nil {
			return
		}
		err = cons.consumeEvent(args)
		if err != nil {
			return
		}
		consList = append(consList, cons)
	}
	return
}

// ConsumeBatch create consumers based on per thread and directly consume messages from the Kafka broker.
// ConsumeBatch is an improved version of Consume but polling in a batch manner.
func (c *Container) ConsumeBatch(config kafka.ConfigMap, args ConsumeArgs) (consList []*Consumer, err error) {
	newConfig := c.cloneConfig(config)
	newConfig[GoEventsChannelEnable] = true
	newConfig[GoApplicationRebalanceEnable] = true
	for numWorker := uint64(1); numWorker <= args.Workers; numWorker++ {
		var cons *Consumer
		cons, err = c.NewConsumer(newConfig)
		if err != nil {
			return
		}
		err = cons.consumeBatch(args)
		if err != nil {
			return
		}
		consList = append(consList, cons)
	}
	return
}

// ConsumeEventBatch create consumers based on per thread and directly consume events from the Kafka broker.
// ConsumeEventBatch is an upgraded version of ConsumeEvent with the batch processing.
func (c *Container) ConsumeEventBatch(config kafka.ConfigMap, args ConsumeArgs) (consList []*Consumer, err error) {
	newConfig := c.cloneConfig(config)
	newConfig[GoEventsChannelEnable] = true
	for numWorker := uint64(1); numWorker <= args.Workers; numWorker++ {
		var cons *Consumer
		cons, err = c.NewConsumer(newConfig)
		if err != nil {
			return
		}
		err = cons.consumeEventBatch(args)
		if err != nil {
			return
		}
		consList = append(consList, cons)
	}
	return
}
