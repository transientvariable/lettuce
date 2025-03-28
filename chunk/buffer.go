package chunk

import (
	"github.com/valyala/bytebufferpool"
)

var (
	byteBufferPool bytebufferpool.Pool
)

// acquireByteBuffer returns a pooled bytebufferpool.ByteBuffer.
func acquireByteBuffer() *bytebufferpool.ByteBuffer {
	return byteBufferPool.Get()
}

// releaseByteBuffer adds the specified bytebufferpool.ByteBuffer back to the pool.
func releaseByteBuffer(b *bytebufferpool.ByteBuffer) {
	if b != nil {
		b.Reset()
		byteBufferPool.Put(b)
	}
}
