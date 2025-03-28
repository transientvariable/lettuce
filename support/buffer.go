package support

import (
	"github.com/transientvariable/support-go"

	pool "github.com/libp2p/go-buffer-pool"
)

const (
	// bufferSize defines the default size for a buffer.
	bufferSizeMax = 4 * support.MiB
)

var (
	bufferPool pool.BufferPool
)

// AcquireBuffer returns a byte slice from the buffer pool with a default size.
func AcquireBuffer() []byte {
	return bufferPool.Get(bufferSizeMax)
}

// AcquireBufferN returns a byte slice of the specified length from the buffer pool.
func AcquireBufferN(size int) []byte {
	if size <= 0 || size > bufferSizeMax {
		return AcquireBuffer()
	}
	return bufferPool.Get(size)
}

// ReleaseBuffer adds the specified byte slice back to the buffer pool.
func ReleaseBuffer(b []byte) {
	capacity := cap(b)
	if capacity > 0 && capacity <= bufferSizeMax {
		bufferPool.Put(b)
	}
}
