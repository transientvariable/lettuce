package chunk

import (
	"errors"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/lettuce/pb/filer_pb"
)

const (
	// Size defines the default size for a chunk buffer.
	Size = 4 * anchor.MiB
)

// Chunk is a container that represents part of the content for a file.
type Chunk struct {
	chunk    *filer_pb.FileChunk
	offset   Offset
	position int
}

// NewChunk creates a new Chunk from the protobuf variant.
func NewChunk(chunk *filer_pb.FileChunk, options ...func(*Chunk)) (Chunk, error) {
	if chunk == nil {
		return Chunk{}, errors.New("chunk: file chunk required")
	}

	c := Chunk{
		chunk: chunk,
		offset: Offset{
			Start: chunk.GetOffset(),
			End:   chunk.GetOffset() + int64(chunk.GetSize()),
		},
	}
	for _, opt := range options {
		opt(&c)
	}
	return c, nil
}

// FileID returns the file ID which represents the coordinates of the Chunk.
func (c Chunk) FileID() string {
	if c.PB() != nil {
		return c.chunk.GetFileId()
	}
	return ""
}

// Offset returns the Offset for the Chunk.
func (c Chunk) Offset() Offset {
	return c.offset
}

// PB returns the underlying protobuf filer_pb.FileChunk.
func (c Chunk) PB() *filer_pb.FileChunk {
	return c.chunk
}

// Position returns the pos of the Chunk relative to others for a single object.
func (c Chunk) Position() int {
	return c.position
}

// Size returns the size of the Chunk.
func (c Chunk) Size() int64 {
	return c.Offset().Length()
}

// ToMap returns a map containing the properties of the Chunk.
func (c Chunk) ToMap() map[string]any {
	return map[string]any{
		"file_id":  c.FileID(),
		"offset":   c.Offset(),
		"position": c.Position(),
		"size":     c.Size(),
	}
}

// String returns a string representation of the Chunk.
func (c Chunk) String() string {
	return string(anchor.ToJSON(c.ToMap()))
}

// WithPosition sets the pos of the Chunk relative to others for a single object.
func WithPosition(pos uint) func(*Chunk) {
	return func(c *Chunk) {
		c.position = int(pos)
	}
}
