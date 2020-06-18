package kafkaclient

// Closer specifies the contract to close all of the underlying resoources.
type Closer interface {
	Close() (err error)
}
