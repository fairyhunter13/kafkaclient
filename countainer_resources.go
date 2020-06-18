package kafkaclient

import (
	"sync/atomic"

	"github.com/panjf2000/ants/v2"
)

// Close all resources regarding the current instance.
func (c *Container) Close() {
	for index := uint64(0); index <= atomic.LoadUint64(&c.resourcesCounter); index++ {
		c.closeSignal <- true
	}
	close(c.closeSignal)
}

func (c *Container) addResourcesCounter() {
	atomic.AddUint64(&c.resourcesCounter, 1)
}

func (c *Container) close(instance Closer) {
	c.addResourcesCounter()
	ants.Submit(func() {
		<-c.closeSignal
		instance.Close()
	})
}
