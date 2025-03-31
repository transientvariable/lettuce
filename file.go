package lettuce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/transientvariable/fs-go"
	"github.com/transientvariable/lettuce/chunk"
	"github.com/transientvariable/lettuce/cluster/filer"
	"github.com/transientvariable/lettuce/support"
	"github.com/transientvariable/log-go"

	gofs "io/fs"
	gohttp "net/http"
)

var (
	_ fs.File     = (*File)(nil)
	_ gohttp.File = (*File)(nil)
)

// File provides access to a single file or directory.
//
// Implements the behavior defined by the fs.File and http.File interfaces.
type File struct {
	client    *gohttp.Client
	closed    bool
	ctx       context.Context
	ctxCancel context.CancelFunc
	ctxParent context.Context
	dirIter   fs.DirIterator
	entry     *filer.Entry
	fileInfo  gofs.FileInfo
	flag      int
	let       *Lettuce
	mutex     sync.Mutex
	reader    io.ReadSeekCloser
	rOff      int64
	wOff      int64
	writer    io.WriteCloser

	blue error
}

func newFile(let *Lettuce, flag int, options ...func(*File)) (*File, error) {
	f := &File{let: let, flag: flag}
	for _, opt := range options {
		opt(f)
	}

	if f.ctxParent == nil {
		f.ctxParent = context.Background()
	}
	f.ctx, f.ctxCancel = context.WithCancel(f.ctxParent)

	if f.entry == nil {
		f.entry = let.entry
		return f, nil
	}

	if !f.entry.IsDir() && flag&fs.O_TRUNC > 0 {
		log.Trace(fmt.Sprintf("[lettuce:file] truncating file ref: \n%s", f.entry))

		path := fsPath(let, f.entry.Path())
		if _, err := let.cluster.Truncate(f.ctx, path); err != nil {
			return nil, err
		}

		e, err := let.cluster.Filer().Stat(f.ctx, path)
		if err != nil {
			return nil, err
		}
		f.entry = e
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	f.fileInfo = fi

	if f.client == nil {
		f.client = let.httpClient
	}

	if err := f.checkRead("newFile"); err == nil {
		f.reader, err = chunk.NewReader(
			let.cluster.Master().FindVolumes,
			f.entry.Chunks(),
			chunk.WithReaderContext(f.ctx))
		if err != nil {
			return nil, err
		}
	}

	if err := f.checkWrite("newFile"); err == nil {
		f.writer, err = chunk.NewWriter(
			f.entry.Path().String(),
			let.cluster.Filer().AssignVolume,
			chunk.WithWriterChunks(f.entry.Chunks()),
			chunk.WithWriterContext(f.ctx))
		if err != nil {
			return nil, err
		}
	}
	return f, nil
}

func (f *File) Close() error {
	if f == nil {
		return gofs.ErrInvalid
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.ctxCancel != nil {
		defer f.ctxCancel()
	}

	if !f.closed {
		f.closed = true
		if err := f.finalize(); err != nil {
			return fmt.Errorf("lettuce_file: %w", &gofs.PathError{Op: "close", Err: err})
		}
		return nil
	}
	return fmt.Errorf("lettuce_file: %w", &gofs.PathError{Op: "close", Err: gofs.ErrClosed})
}

func (f *File) Read(b []byte) (int, error) {
	if err := f.checkRead("read"); err != nil {
		return 0, err
	}

	if len(b) == 0 {
		return 0, nil
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.rOff >= f.fileInfo.Size() {
		return 0, io.EOF
	}

	n, err := f.reader.Read(b)
	if err != nil {
		return 0, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   "read",
			Path: f.fileInfo.Name(),
			Err:  err,
		})
	}
	f.rOff += int64(n)
	return n, nil
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	if err := f.checkRead("readAt"); err != nil {
		return 0, err
	}

	if len(b) == 0 {
		return 0, nil
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	s, err := f.reader.Seek(off, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   "readAt",
			Path: f.fileInfo.Name(),
			Err:  err,
		})
	}
	f.rOff = s

	n, err := f.reader.Read(b)
	if err != nil {
		return n, err
	}
	f.rOff += int64(n)

	if n < len(b) && f.rOff >= f.fileInfo.Size() {
		return n, io.EOF
	}
	return n, nil
}

func (f *File) ReadFrom(r io.Reader) (int64, error) {
	if err := f.checkWrite("readFrom"); err != nil {
		return 0, err
	}

	if r == nil {
		return 0, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   "readFrom",
			Path: f.fileInfo.Name(),
			Err:  errors.New("reader is nil"),
		})
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	bufSize := chunk.Size
	if bufSize > int(f.fileInfo.Size()) {
		bufSize = int(f.fileInfo.Size())
	}

	buf := support.AcquireBufferN(bufSize)
	defer support.ReleaseBuffer(buf)

	n, err := io.CopyBuffer(f.writer, r, buf)
	if err != nil {
		return n, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   "readFrom",
			Path: f.fileInfo.Name(),
			Err:  err,
		})
	}
	f.wOff += n
	return n, nil
}

func (f *File) Readdir(count int) ([]gofs.FileInfo, error) {
	de, err := f.readDir(count)
	entries := make([]gofs.FileInfo, len(de))
	for i, e := range de {
		entries[i] = e
	}
	return entries, err
}

func (f *File) ReadDir(n int) ([]gofs.DirEntry, error) {
	de, err := f.readDir(n)
	entries := make([]gofs.DirEntry, len(de))
	for i, e := range de {
		entries[i] = e
	}
	return entries, err
}

func (f *File) Seek(off int64, whence int) (int64, error) {
	if err := f.checkRead("seek"); err != nil {
		return 0, err
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	s, err := f.reader.Seek(off, whence)
	if err != nil {
		return 0, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   "seek",
			Path: f.fileInfo.Name(),
			Err:  err,
		})
	}
	f.rOff = s
	return s, nil
}

func (f *File) Stat() (gofs.FileInfo, error) {
	if f == nil {
		return nil, gofs.ErrInvalid
	}

	if f.closed {
		return nil, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   "stat",
			Path: f.fileInfo.Name(),
			Err:  gofs.ErrClosed,
		})
	}

	e, err := FSEntry(f.let, f.entry)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (f *File) Sync() error {
	return nil
}

func (f *File) Write(b []byte) (int, error) {
	if err := f.checkWrite("write"); err != nil {
		return 0, err
	}

	if len(b) == 0 {
		return 0, nil
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	n, err := f.writer.Write(b)
	if err != nil {
		return n, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   "write",
			Path: f.fileInfo.Name(),
			Err:  err,
		})
	}
	f.wOff += int64(n)
	return n, nil
}

// String returns a string representation of a File.
func (f *File) String() string {
	return ""
}

func (f *File) checkRegularFile(op string) error {
	if f.entry.IsDir() {
		return fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   op,
			Path: f.entry.Name(),
			Err:  errors.New("is a directory"),
		})
	}
	return nil
}

func (f *File) checkRead(op string) error {
	if err := f.checkRegularFile(op); err != nil {
		return err
	}

	if f.flag == fs.O_WRONLY {
		return fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   op,
			Path: f.entry.Name(),
			Err:  errors.New("file is write-only"),
		})
	}
	return nil
}

func (f *File) checkWrite(op string) error {
	if err := f.checkRegularFile(op); err != nil {
		return err
	}

	if f.flag == fs.O_RDONLY {
		return fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   op,
			Path: f.entry.Name(),
			Err:  errors.New("file is read-only"),
		})
	}
	return nil
}

func (f *File) finalize() error {
	var err error
	if f.reader != nil {
		err = errors.Join(f.reader.Close())
	}

	if f.writer != nil {
		err = errors.Join(err, f.writer.Close())
		if f.wOff > 0 {
			//sort.Stable(f.entry.Chunks())
			err = errors.Join(err, f.let.cluster.Filer().Update(f.ctx, f.entry))
		}
	}
	return err
}

func (f *File) readDir(n int) ([]*fs.Entry, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return nil, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
			Op:   "readDir",
			Path: fi.Name(),
			Err:  fs.ErrNotDir,
		})
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.dirIter == nil {
		iter, err := newDirIterator(f.ctx, f.let, f.entry)
		if err != nil {
			return nil, fmt.Errorf("lettuce_file: %w", &gofs.PathError{
				Op:   "readDir",
				Path: fi.Name(),
				Err:  err,
			})
		}
		f.dirIter = iter
	}
	return f.dirIter.NextN(n)
}
