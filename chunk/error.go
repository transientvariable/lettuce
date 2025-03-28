package chunk

import (
	"fmt"
	"net/url"
)

// Enumeration of errors that may be returned by chunk operations.
const (
	ErrChunkNotFound   = chunkError("chunk not found")
	ErrInvalidRange    = chunkError("invalid range")
	ErrInvalidOp       = chunkError("invalid operation")
	ErrVolumesNotFound = chunkError("volumes not found")
)

// chunkError defines the type for errors that may be returned by chunk operations.
type chunkError string

// Error returns the cause of a chunk operation error.
func (e chunkError) Error() string {
	return string(e)
}

// ContentLengthError records an error when the content length of a response for chunk content does not match the chunks
// size.
type ContentLengthError struct {
	Chunk         Chunk   `json:"chunk"`
	ContentLength int64   `json:"content_length"`
	Location      url.URL `json:"location,omitempty"`
	Op            string  `json:"operation,omitempty"`
	Path          string  `path:"path,omitempty"`
}

func (e *ContentLengthError) Error() string {
	return fmt.Sprintf("expected content with length %d, but received %d for chunk: location=%s, offset=%s, path=%s",
		e.Chunk.Size(),
		e.ContentLength,
		e.Location.String(),
		e.Chunk.Offset(),
		e.Path)
}
