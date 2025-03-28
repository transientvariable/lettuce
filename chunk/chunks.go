package chunk

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/transientvariable/collection"
	"github.com/transientvariable/collection/list"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log"
	"github.com/transientvariable/support"
)

// OnAdd defines the signature for the function to call when new chunks are add to Chunks.
type OnAdd func(*Chunks) error

// Chunks is container for a collection chunks representing file content.
type Chunks struct {
	chunks       map[Offset]Chunk
	chunkSizeMax int64
	chunkSizeMin int64
	mutex        sync.Mutex
	onAdd        OnAdd
	path         string
	size         int64
}

// NewChunks creates a new container for a collection of chunks representing file content.
func NewChunks(path string, options ...func(*Chunks)) (*Chunks, error) {
	if path = strings.TrimSpace(path); path == "" {
		return nil, errors.New("chunks: path is required")
	}

	c := &Chunks{chunks: make(map[Offset]Chunk), path: path}
	for _, opt := range options {
		opt(c)
	}
	return c, nil
}

// Add adds one or more protobuf chunks to Chunks.
func (c *Chunks) Add(chunks ...*filer_pb.FileChunk) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var p int
	if c.Len() > 0 {
		cks, err := c.List()
		if err != nil {
			return 0, err
		}

		ck, err := cks.ValueAt(c.Len() - 1)
		if err != nil {
			return 0, err
		}
		p = ck.Position() + 1
	}

	var n int
	for _, fc := range chunks {
		off := Offset{Start: fc.GetOffset(), End: fc.GetOffset() + int64(fc.GetSize())}
		if _, ok := c.chunks[off]; !ok {
			ck, err := NewChunk(fc, WithPosition(uint(p)))
			if err != nil {
				return n, err
			}

			c.chunks[off] = ck
			c.setChunkMinMax(ck.Size())
			c.size += ck.Size()
			n++
			p++
		}
	}

	if c.onAdd != nil {
		if err := c.onAdd(c); err != nil {
			return n, err
		}
	}
	return n, nil
}

// AtOffset returns the Chunk that contains the provided offset, where offset > Chunk.OffsetStart and
// offset < Chunk.OffsetEnd.
//
// If the provided offset does not match the offset interval for a Chunk a ErrChunkNotFound error is returned.
func (c *Chunks) AtOffset(offset int64) (Chunk, error) {
	if offset < 0 || offset > c.Size() {
		return Chunk{}, errors.New(fmt.Sprintf("chunks: invalid offset %d for chunks with size %d", offset, c.Size()))
	}

	off, err := find(offset, c.chunks)
	if err != nil {
		return Chunk{}, err
	}
	return c.chunks[off], nil
}

// ChunkSizeMin returns the size in bytes of the smallest Chunk.
func (c *Chunks) ChunkSizeMin() int64 {
	return c.chunkSizeMin
}

// ChunkSizeMax returns the size in bytes of the largest Chunk.
func (c *Chunks) ChunkSizeMax() int64 {
	return c.chunkSizeMax
}

// Clear resets the state of all the internal properties of Chunks to their zero values.
func (c *Chunks) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for o := range c.chunks {
		delete(c.chunks, o)
	}
	c.chunkSizeMin = 0
	c.chunkSizeMax = 0
	c.size = 0
}

// List ...
func (c *Chunks) List() (list.List[Chunk], error) {
	var cks list.List[Chunk]
	for off := range c.chunks {
		if err := cks.Add(c.chunks[off]); err != nil {
			if err != nil {
				return cks, err
			}
		}
	}
	sort.Slice(cks, func(i int, j int) bool { return cks[i].Offset().Before(cks[j].Offset()) })
	return cks, nil
}

// Iterate returns a collection.Iterator that emits each Chunk in sequence order.
func (c *Chunks) Iterate() (collection.Iterator[Chunk], error) {
	cks, err := c.List()
	if err != nil {
		return nil, err
	}
	return cks.Iterate(), nil
}

// Len returns the number of chunks.
func (c *Chunks) Len() int {
	return len(c.chunks)
}

// Path returns the path to the object that Chunks represents.
func (c *Chunks) Path() string {
	return c.path
}

// PB returns the list of protobuf filer_pb.FileChunk.
func (c *Chunks) PB() ([]*filer_pb.FileChunk, error) {
	values := make([]*filer_pb.FileChunk, c.Len())
	iter, err := c.Iterate()
	if err != nil {
		return values, err
	}

	var i int
	for iter.HasNext() {
		n, err := iter.Next()
		if err != nil {
			if !errors.Is(err, collection.ErrNoMoreElements) {
				return values, err
			}
		}
		values[i] = n.PB()
		i++
	}
	return values, nil
}

// Size returns the total size in bytes for all the chunks.
func (c *Chunks) Size() int64 {
	return c.size
}

// Values returns the chunks a slice.
func (c *Chunks) Values() []Chunk {
	cks, err := c.List()
	if err != nil {
		return []Chunk{}
	}
	return cks.Values()
}

// ToMap returns a map representing the properties of Chunks.
func (c *Chunks) ToMap() map[string]any {
	m := make(map[string]any)
	m["chunk_size"] = map[string]any{
		"min": c.ChunkSizeMin(),
		"max": c.ChunkSizeMax(),
	}
	m["length"] = c.Len()
	m["path"] = c.Path()
	m["size"] = c.Size()

	cks, err := c.List()
	if err != nil {
		m["chunks"] = err.Error()
		return m
	}

	v := make([]map[string]any, c.Len())
	for i, ck := range cks.Values() {
		v[i] = ck.ToMap()
	}
	m["chunks"] = v
	return m
}

// String returns a string representation of the Chunks.
func (c *Chunks) String() string {
	return string(support.ToJSON(c.ToMap()))
}

func (c *Chunks) chunksAt(i int, j int) (Chunk, Chunk, error) {
	cks, err := c.List()
	if err != nil {
		return Chunk{}, Chunk{}, err
	}

	c1, err := cks.ValueAt(i)
	if err != nil {
		return Chunk{}, Chunk{}, err
	}

	c2, err := cks.ValueAt(j)
	if err != nil {
		return c1, Chunk{}, err
	}
	return c1, c2, nil
}

func (c *Chunks) setChunkMinMax(size int64) {
	if c.chunkSizeMin == 0 {
		c.chunkSizeMin = size
	}
	if c.chunkSizeMax == 0 {
		c.chunkSizeMax = size
	}
	if size > c.chunkSizeMax {
		c.chunkSizeMax = size
	}
	if size < c.chunkSizeMax {
		c.chunkSizeMin = size
	}
}

func find(offset int64, chunks map[Offset]Chunk) (Offset, error) {
	for o := range chunks {
		if o.Contains(offset) {
			return o, nil
		}
	}
	return Offset{}, ErrChunkNotFound
}

// WithEntry ...
func WithEntry(entry *filer_pb.Entry) func(*Chunks) {
	return func(c *Chunks) {
		if entry != nil {
			if _, err := c.Add(entry.GetChunks()...); err != nil {
				log.Error("[seaweedfs:chunks]", log.Err(err))
			}
		}
	}
}

// WithOnAdd ...
func WithOnAdd(fn OnAdd) func(*Chunks) {
	return func(c *Chunks) {
		c.onAdd = fn
	}
}
