package chunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"sync"

	"github.com/transientvariable/collection"
	"github.com/transientvariable/log"
	"github.com/transientvariable/net/http"

	"github.com/valyala/bytebufferpool"

	weedsprt "github.com/transientvariable/lettuce/support"
	gohttp "net/http"
)

const (
	// DefaultReaderQueueSize sets the number of chunks to buffer in memory.
	DefaultReaderQueueSize = 8
)

var (
	_ io.ReadSeekCloser = (*Reader)(nil)

	rcPool = sync.Pool{
		New: func() any { return &rc{} },
	}
)

type rc struct {
	chunk   Chunk
	content *bytebufferpool.ByteBuffer
	discard int
	err     error
	size    int
}

// FindVolumes defines the function signature for retrieving the list of volume locations containing data for a Chunk.
type FindVolumes func(context.Context, string, string) ([]url.URL, error)

// Reader reads Chunk content for a file.
type Reader struct {
	buf       *bytes.Buffer
	chunks    *Chunks
	closed    bool
	ctx       context.Context
	ctxCancel context.CancelFunc
	ctxParent context.Context
	err       error
	findVols  FindVolumes
	mutex     sync.RWMutex
	offset    int64
	path      string
	position  int
	queue     <-chan chan *rc
	queueSize int
	size      int64
}

// NewReader creates a new Reader using the provided FindVolumes function and Chunks.
func NewReader(findVols FindVolumes, chunks *Chunks, option ...func(*Reader)) (*Reader, error) {
	if findVols == nil {
		return nil, errors.New("chunk_reader: func for locating volumes is required")
	}

	if chunks == nil {
		return nil, errors.New("chunk_reader: chunks cannot be nil or empty")
	}

	r := &Reader{
		buf:      &bytes.Buffer{},
		chunks:   chunks,
		findVols: findVols,
		path:     chunks.Path(),
		size:     chunks.Size(),
	}
	for _, opt := range option {
		opt(r)
	}

	if r.queueSize <= 0 {
		r.queueSize = DefaultReaderQueueSize
	}

	if chunks.Size() <= chunks.ChunkSizeMax() {
		r.queueSize = 1
	}

	if r.ctxParent == nil {
		r.ctxParent = context.Background()
	}
	r.ctx, r.ctxCancel = context.WithCancel(r.ctxParent)

	if r.chunks.Len() > 0 {
		if err := r.init(0); err != nil {
			return nil, fmt.Errorf("chunks_reader: %w", err)
		}
	}
	return r, nil
}

func (r *Reader) Close() error {
	if r == nil {
		return ErrInvalidOp
	}

	if r.err != nil {
		return r.err
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.closed {
		r.closed = true
		if r.ctxCancel != nil {
			r.ctxCancel()
		}
		return nil
	}
	return errors.New("chunk_reader: already closed")
}

func (r *Reader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	if r.err != nil {
		return 0, r.err
	}

	if r.offset >= r.size {
		return 0, io.EOF
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	var n int
	if r.buf.Len() > 0 {
		w, err := r.buf.Read(b)
		if err != nil {
			return n, r.setErr(err)
		}
		n += w
	}

	if n < len(b) && int64(n)+r.offset < r.size {
		r.buf.Reset()
		cb := &bytes.Buffer{}
		for c := range r.queue {
			rc := <-c
			if rc.err != nil {
				return n, r.setErr(rc.err)
			}

			if _, err := r.read(rc, cb); err != nil {
				return n, r.setErr(err)
			}

			if cb.Len() > 0 {
				w, err := cb.Read(b[n:])
				if err != nil {
					return n, r.setErr(err)
				}
				n += w
			}

			if n == len(b) {
				if cb.Len() > 0 {
					if _, err := r.buf.Write(cb.Bytes()); err != nil {
						return n, r.setErr(err)
					}
				}
				break
			}
		}
	}
	r.offset += int64(n)
	if r.offset >= r.size && r.ctxCancel != nil {
		r.ctxCancel()
	}
	return n, nil
}

func (r *Reader) Seek(off int64, whence int) (int64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	switch whence {
	case io.SeekStart:
		// no-ops
	case io.SeekCurrent:
		off += r.offset
	case io.SeekEnd:
		off += r.size
	default:
		return 0, errors.New(fmt.Sprintf("invalid whence: %d", whence))
	}

	if off < 0 {
		return off, errors.New("negative pos")
	}

	if off != r.offset && off <= r.size {
		r.ctxCancel()
		r.buf.Reset()
		if err := r.init(off); err != nil {
			return 0, err
		}
	}
	return off, nil
}

func (r *Reader) buffer(ctx context.Context, iter collection.Iterator[Chunk]) <-chan chan *rc {
	queue := make(chan chan *rc)
	go func() {
		defer close(queue)
		for iter.HasNext() {
			c, err := iter.Next()
			select {
			case queue <- r.acquireChunk(ctx, c, 0, err):
			case <-ctx.Done():
				return
			}
		}
	}()
	return queue
}

func (r *Reader) get(ctx context.Context, c Chunk) (*bytebufferpool.ByteBuffer, error) {
	locs, err := r.find(ctx, c)
	if err != nil {
		return nil, err
	}

	var (
		loc    url.URL
		volIdx int
	)
	if len(locs) == 1 {
		loc = locs[volIdx]
	} else {
		loc = locs[int(uint(rand.Intn(len(locs)-1)))]
	}

	req, err := gohttp.NewRequest(http.MethodGet, loc.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DoWithRetry(httpClient(), req)
	defer func(resp *gohttp.Response) {
		if resp != nil && resp.Body != nil {
			if err := resp.Body.Close(); err != nil {
				log.Error("[chunk:reader]", log.Err(err))
			}
		}
	}(resp)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case gohttp.StatusOK, gohttp.StatusPartialContent:
		break
	case gohttp.StatusRequestedRangeNotSatisfiable:
		return nil, fmt.Errorf("request failed %s: %w", req.URL.String(), ErrInvalidRange)
	default:
		return nil, errors.New(fmt.Sprintf("request failed %s: %s", req.URL.String(), resp.Status))
	}

	b := acquireByteBuffer()
	buf := weedsprt.AcquireBufferN(int(r.chunks.ChunkSizeMax()))
	defer weedsprt.ReleaseBuffer(buf)

	w, err := io.CopyBuffer(b, resp.Body, buf)
	if err != nil {
		releaseByteBuffer(b)
		return nil, err
	}

	if w != c.Size() {
		releaseByteBuffer(b)
		return nil, &ContentLengthError{
			Op:            "get",
			Chunk:         c,
			ContentLength: w,
			Location:      loc,
			Path:          r.path,
		}
	}
	return b, nil
}

func (r *Reader) init(off int64) error {
	if r.ctxCancel != nil {
		r.ctxCancel()
	}

	cks, err := r.chunks.List()
	if err != nil {
		return err
	}

	c, err := cks.ValueAt(0)
	if err != nil {
		return err
	}

	if off > 0 && off <= r.size {
		c, err = r.chunks.AtOffset(off)
		if err != nil {
			return err
		}
	}

	iter := cks.Iterate()
	if c.Position() > 0 {
		r.position = c.Position()
		if err := discard(iter, c.Position()); err != nil {
			return err
		}
	}

	r.ctx, r.ctxCancel = context.WithCancel(r.ctxParent)
	c, err = iter.Next()
	rc := <-r.acquireChunk(r.ctx, c, off, err)
	if _, err := r.read(rc, r.buf); err != nil {
		return err
	}

	r.offset = off
	if iter.HasNext() {
		r.queue = r.buffer(r.ctx, iter)
	}
	return nil
}

func (r *Reader) find(ctx context.Context, c Chunk) ([]url.URL, error) {
	vols, err := r.findVols(ctx, "", c.FileID())
	if err != nil {
		return nil, err
	}

	if len(vols) == 0 {
		return nil, fmt.Errorf("%w: fileID=%s", ErrVolumesNotFound, c.FileID())
	}

	var locations []url.URL
	for _, vol := range vols {
		if v, loc, err := location(c, vol); err == nil && v {
			locations = append(locations, loc)
		}
	}

	if len(locations) == 0 {
		return nil, fmt.Errorf("%w: fileID=%s", ErrVolumesNotFound, c.FileID())
	}
	return locations, nil
}

func location(c Chunk, vol url.URL) (bool, url.URL, error) {
	path, err := url.JoinPath(vol.Path, c.FileID())
	if err != nil {
		return false, vol, err
	}
	vol.Path = path

	req, err := gohttp.NewRequest(http.MethodHead, vol.String(), nil)
	if err != nil {
		return false, vol, err
	}

	resp, err := httpClient().Do(req)
	defer func(resp *gohttp.Response) {
		if resp != nil && resp.Body != nil {
			if err := resp.Body.Close(); err != nil {
				log.Error("[chunk:reader]", log.Err(err))
			}
		}
	}(resp)
	if err != nil {
		return false, vol, err
	}

	if resp.StatusCode >= gohttp.StatusOK && resp.StatusCode < gohttp.StatusBadRequest {
		return true, vol, nil
	}
	return false, vol, nil
}

func (r *Reader) read(c *rc, w *bytes.Buffer) (int64, error) {
	if c == nil {
		return 0, nil
	}

	defer r.releaseChunk(c)
	if c.err != nil {
		return 0, c.err
	}

	if c.content == nil || c.content.Len() <= c.discard {
		return 0, nil
	}

	n, err := c.content.WriteTo(w)
	if err != nil {
		return n, err
	}
	return n, nil
}

// acquireChunk returns an internal chunk used for Reader operations.
func (r *Reader) acquireChunk(ctx context.Context, c Chunk, off int64, err error) chan *rc {
	chunk := make(chan *rc)
	go func() {
		defer close(chunk)
		rc := rcPool.Get().(*rc)
		rc.chunk = c
		if err != nil {
			rc.err = err
		}

		if rc.err == nil {
			rc.content, rc.err = r.get(ctx, rc.chunk)
			if rc.err == nil && rc.content.Len() > 0 && off > 0 {
				b := rc.content.Bytes()
				rc.content.Reset()
				_, rc.err = rc.content.Write(b[off-rc.chunk.Offset().Start:])
			}
		}
		chunk <- rc
	}()
	return chunk
}

// releaseChunk adds the internal Reader chunk back to the pool.
func (r *Reader) releaseChunk(c *rc) {
	if c != nil {
		c.chunk = Chunk{}
		if c.content != nil {
			releaseByteBuffer(c.content)
			c.content = nil
		}
		c.discard = 0
		c.err = nil
		c.size = 0
		rcPool.Put(c)
	}
}

func (r *Reader) setErr(err error) error {
	if r.err == nil {
		r.err = err
	}
	return r.err
}

func discard(iter collection.Iterator[Chunk], p int) error {
	for i := 0; iter.HasNext() && i < p; i++ {
		if _, err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

// WithReaderContext ...
func WithReaderContext(ctx context.Context) func(*Reader) {
	return func(r *Reader) {
		r.ctxParent = ctx
	}
}

// WithReaderQueueSize ...
func WithReaderQueueSize(size uint) func(*Reader) {
	return func(r *Reader) {
		r.queueSize = int(size)
	}
}
