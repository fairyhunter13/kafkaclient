package kafkaclient

import (
	"sync"
	"sync/atomic"
)

// Close all resources regarding the current instance.
func (c *Container) Close() {
	var wg sync.WaitGroup
	for index := uint64(1); index <= atomic.LoadUint64(&c.resourcesCounter); index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.closeSignal <- true
		}()
	}
	wg.Wait()
	close(c.closeSignal)
}

func (c *Container) addResourcesCounter() {
	atomic.AddUint64(&c.resourcesCounter, 1)
}

func (c *Container) close(instance Closer) {
	c.addResourcesCounter()
	go func(instance Closer) {
		<-c.closeSignal
		instance.Close()
	}(instance)
}
