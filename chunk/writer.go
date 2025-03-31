package chunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/transientvariable/anchor/net/http"
	"github.com/transientvariable/lettuce/support"
	"github.com/transientvariable/log-go"
	"github.com/valyala/bytebufferpool"

	gohttp "net/http"
)

var (
	_ io.WriteCloser = (*Writer)(nil)

	quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

	wcPool = sync.Pool{
		New: func() any { return &wc{} },
	}
)

type wc struct {
	fileID  string
	content []byte
	err     error
	loc     url.URL
	offset  int64
}

// AssignVolume ...
type AssignVolume func(context.Context, string) (string, url.URL, error)

// Writer ...
type Writer struct {
	assignVol AssignVolume
	buf       *bytes.Buffer
	chunks    *Chunks
	chunkSize int
	closed    bool
	ctx       context.Context
	ctxCancel context.CancelFunc
	ctxParent context.Context
	err       error
	mutex     sync.Mutex
	offset    int64
	path      string
	queue     chan []byte
	wgBuf     sync.WaitGroup
	wgWrite   sync.WaitGroup
}

// NewWriter ...
func NewWriter(path string, assignVol AssignVolume, option ...func(*Writer)) (*Writer, error) {
	if path := strings.TrimSpace(path); path == "" {
		return nil, errors.New("chunk_writer: path is required")
	}

	if assignVol == nil {
		return nil, errors.New("chunk_writer: func for assigning volumes is required")
	}

	w := &Writer{assignVol: assignVol, buf: &bytes.Buffer{}, path: path}
	for _, opt := range option {
		opt(w)
	}

	if w.chunkSize <= 0 {
		w.chunkSize = Size
	}

	if w.ctxParent == nil {
		w.ctxParent = context.Background()
	}

	w.ctx, w.ctxCancel = context.WithCancel(w.ctxParent)
	w.queue = make(chan []byte)
	w.buffer(w.ctx, w.queue)
	return w, nil
}

func (w *Writer) Close() error {
	if w == nil {
		return ErrInvalidOp
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()
	close(w.queue)
	w.wgWrite.Wait()

	if w.err != nil {
		return w.err
	}

	if !w.closed {
		w.closed = true
		w.err = errors.New("chunk_writer: already closed")
		if w.buf.Len() > 0 {
			if err := w.write(w.ctx, w.buf); err != nil {
				return err
			}
		}
		return nil
	}
	return w.err
}

func (w *Writer) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.err != nil {
		return 0, w.err
	}
	w.wgBuf.Add(1)
	w.queue <- b
	w.wgBuf.Wait()
	return len(b), nil
}

func (w *Writer) buffer(ctx context.Context, queue <-chan []byte) {
	go func() {
		for b := range queue {
			select {
			case <-ctx.Done():
				return
			default:
				if _, err := w.buf.Write(b); err != nil {
					w.setErr(err)
					w.wgBuf.Done()
					return
				}
				w.wgBuf.Done()

				if w.buf.Len() >= w.chunkSize {
					if err := w.write(ctx, w.buf); err != nil {
						w.setErr(err)
						return
					}
				}
			}
		}
	}()
}

func (w *Writer) write(ctx context.Context, buf *bytes.Buffer) error {
	w.wgWrite.Add(1)
	defer w.wgWrite.Done()

	for buf.Len() >= w.chunkSize {
		c := w.acquireChunk(ctx, w.chunkSize, w.offset)
		if c.err != nil {
			return c.err
		}

		n, err := buf.Read(c.content)
		if err != nil {
			return err
		}

		if err := w.writeChunk(c); err != nil {
			return err
		}
		w.offset += int64(n)
	}

	if w.closed && buf.Len() > 0 {
		c := w.acquireChunk(ctx, buf.Len(), w.offset)
		if c.err != nil {
			return c.err
		}

		n, err := buf.Read(c.content)
		if err != nil {
			return err
		}

		if err := w.writeChunk(c); err != nil {
			return err
		}
		w.offset += int64(n)
		return nil
	}

	b := buf.Bytes()
	buf.Reset()
	if len(b) > 0 {
		if _, err := buf.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeChunk(c *wc) error {
	defer w.releaseChunk(c)
	ts := time.Now()
	r, err := w.upload(c)
	if err != nil {
		return err
	}

	fc, err := r.FileChunk(c.fileID, c.offset, ts.UnixNano())
	if err != nil {
		return err
	}

	if w.chunks != nil {
		if _, err := w.chunks.Add(fc); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) upload(c *wc) (UploadResult, error) {
	buf := acquireByteBuffer()
	defer releaseByteBuffer(buf)

	var r UploadResult
	ct, err := w.createFormFile(c.content, c.loc, buf)
	if err != nil {
		return r, err
	}

	req, err := gohttp.NewRequest(http.MethodPost, c.loc.String(), bytes.NewReader(buf.Bytes()))
	if err != nil {
		return r, err
	}
	req.Header.Set(http.HeaderContentType, ct)
	req.Header.Set(http.HeaderRange, fmt.Sprintf("bytes=%d-", c.offset))

	resp, err := http.DoWithRetry(httpClient(), req)
	defer func(resp *gohttp.Response) {
		if resp != nil && resp.Body != nil {
			if err := resp.Body.Close(); err != nil {
				log.Error("[chunk:writer]", log.Err(err))
			}
		}
	}(resp)
	if err != nil {
		return UploadResult{}, err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return r, fmt.Errorf("request failed for addr %s: %s", req.URL.String(), resp.Status)
	}

	r, err = decodeUploadResponse(resp)
	if err != nil {
		return r, err
	}
	return r, nil
}

func (w *Writer) createFormFile(c []byte, loc url.URL, buf *bytebufferpool.ByteBuffer) (string, error) {
	h := make(textproto.MIMEHeader)
	h.Set(http.HeaderContentDisposition, fmt.Sprintf(`form-data; name="file"; filename="%s"`, escapeQuotes(w.path)))
	h.Set(http.HeaderIdempotencyKey, loc.String())

	mw := multipart.NewWriter(buf)
	cw, err := mw.CreatePart(h)
	if err != nil {
		return "", err
	}

	if _, err := cw.Write(c); err != nil {
		return "", err
	}

	ct := mw.FormDataContentType()
	if err := mw.Close(); err != nil {
		return "", err
	}
	return ct, nil
}

func (w *Writer) setErr(err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.err == nil && err != nil {
		w.err = err
	}
}

// acquireChunk returns an internal chunk used for Writer operations.
func (w *Writer) acquireChunk(ctx context.Context, s int, off int64) *wc {
	c := wcPool.Get().(*wc)
	c.fileID, c.loc, c.err = w.assignVol(ctx, w.chunks.Path())
	if c.err != nil {
		return c
	}
	c.content = support.AcquireBufferN(s)
	c.offset = off
	return c
}

// releaseChunk adds the internal Writer chunk back to the pool.
func (w *Writer) releaseChunk(c *wc) {
	if c != nil {
		if c.content != nil {
			support.ReleaseBuffer(c.content)
		}
		c.content = nil
		c.fileID = ""
		c.err = nil
		c.loc = url.URL{}
		c.offset = 0
		wcPool.Put(c)
	}
}

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}

// WithWriterChunks ...
func WithWriterChunks(chunks *Chunks) func(*Writer) {
	return func(w *Writer) {
		w.chunks = chunks
	}
}

// WithWriterChunkSize ...
func WithWriterChunkSize(size uint) func(*Writer) {
	return func(w *Writer) {
		w.chunkSize = int(size)
	}
}

// WithWriterContext ...
func WithWriterContext(ctx context.Context) func(*Writer) {
	return func(w *Writer) {
		w.ctxParent = ctx
	}
}
