package lettuce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/anchor/net/http"
	"github.com/transientvariable/fs-go"
	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/cluster"
	"github.com/transientvariable/lettuce/cluster/filer"
	"github.com/transientvariable/log-go"

	gofs "io/fs"
	gohttp "net/http"
	goos "os"
)

const (
	modeCreate = 0775
)

var (
	_ fs.FS = (*SeaweedFS)(nil)
)

// SeaweedFS is a file system provider that implements fs.FS using SeaweedFS for the storage backend.
type SeaweedFS struct {
	closed     bool
	cluster    *cluster.Cluster
	entry      *filer.Entry
	gid        int32
	httpClient *gohttp.Client
	mutex      sync.Mutex
	uid        int32
}

// New creates a new fs.FS backed by SeaweedFS using the provided options.
func New(options ...func(*SeaweedFS)) (*SeaweedFS, error) {
	weed := &SeaweedFS{httpClient: http.DefaultClient()}
	for _, opt := range options {
		opt(weed)
	}

	if weed.cluster == nil {
		log.Warn("[seaweedfs] cluster configuration not provided, using default")

		c, err := cluster.New()
		if err != nil {
			return nil, fmt.Errorf("seaweedfs: %w", err)
		}
		weed.cluster = c
	}
	weed.entry = weed.cluster.Filer().Root().Entry()

	if weed.gid <= 0 {
		weed.gid = client.GID
	}

	if weed.uid <= 0 {
		weed.uid = client.UID
	}
	return weed, nil
}

// Close releases any resources used by SeaweedFS.
func (s *SeaweedFS) Close() error {
	log.Debug("[seaweedfs] close")

	if s == nil {
		return gofs.ErrInvalid
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.entry != nil && s.entry.Name() != s.PathSeparator() {
		return nil
	}

	if !s.closed {
		s.closed = true
		if s.cluster != nil {
			if err := s.cluster.Close(); err != nil && !errors.Is(err, gofs.ErrClosed) {
				return err
			}
		}
		return s.cluster.Close()
	}
	return fmt.Errorf("seaweedfs: %w", gofs.ErrClosed)
}

// Cluster returns the cluster API used for performing operations against a SeaweedFS backend.
func (s *SeaweedFS) Cluster() *cluster.Cluster {
	// TODO: Check if FS is closed?
	return s.cluster
}

// Create ...
func (s *SeaweedFS) Create(name string) (fs.File, error) {
	log.Debug("[seaweedfs] create", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := open(ctx, s, name, fs.O_RDWR|fs.O_CREATE|fs.O_TRUNC, modeCreate)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "create", Path: name, Err: err})
	}
	return f, nil
}

// Glob ...
func (s *SeaweedFS) Glob(pattern string) ([]string, error) {
	log.Debug("[seaweedfs] glob", log.String("pattern", pattern))

	var matches []string
	err := gofs.WalkDir(s, `.`, func(path string, entry gofs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if fs.EndsWithDot(s, path) {
			return nil
		}

		matched, err := filepath.Match(pattern, path)
		if err != nil {
			return err
		}

		if matched {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		if !errors.Is(err, &gofs.PathError{}) {
			return matches, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "glob", Err: err})
		}
		return matches, err
	}
	return matches, nil
}

// Mkdir creates a new directory with the specified name and permission bits.
func (s *SeaweedFS) Mkdir(name string, perm gofs.FileMode) error {
	log.Debug("[seaweedfs] mkdir", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := mkdir(ctx, s, name, perm); err != nil {
		return fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "mkdir", Path: name, Err: err})
	}
	return nil
}

// MkdirAll ...
func (s *SeaweedFS) MkdirAll(path string, mode gofs.FileMode) error {
	log.Debug("[seaweedfs] mkdirAll",
		log.String("path", path),
		log.String("mode", mode.String()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := mkdirAll(ctx, s, path, mode); err != nil {
		return fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "mkdirAll", Path: path, Err: err})
	}
	return nil
}

// Open ...
func (s *SeaweedFS) Open(name string) (gofs.File, error) {
	log.Debug("[seaweedfs] open", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := open(ctx, s, name, fs.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "open", Path: name, Err: err})
	}
	return f, nil
}

// OpenFile ...
func (s *SeaweedFS) OpenFile(name string, flag int, mode gofs.FileMode) (fs.File, error) {
	log.Debug("[seaweedfs] openFile",
		log.String("name", name),
		log.Int("flag", flag),
		log.String("mode", mode.String()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := open(ctx, s, name, flag, mode)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "openFile", Path: name, Err: err})
	}
	return f, nil
}

// PathSeparator ...
func (s *SeaweedFS) PathSeparator() string {
	return s.cluster.Filer().PathSeparator()
}

// Provider ...
func (s *SeaweedFS) Provider() string {
	return "seaweedfs"
}

// ReadDir ...
func (s *SeaweedFS) ReadDir(name string) ([]gofs.DirEntry, error) {
	log.Debug("[seaweedfs] readDir", log.String("current_dir", s.entry.Name()), log.String("name", name))

	sub, err := s.Sub(name)
	if err != nil {
		return nil, err
	}
	dir := sub.(*SeaweedFS)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	list, err := newDirIterator(ctx, s, dir.entry)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "readDir", Path: dir.entry.Name(), Err: err})
	}

	de, err := list.NextN(-1)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "readDir", Path: dir.entry.Name(), Err: err})
	}

	entries := make([]gofs.DirEntry, len(de))
	for i, e := range de {
		entries[i] = e
	}
	return entries, nil
}

// ReadFile ...
func (s *SeaweedFS) ReadFile(name string) ([]byte, error) {
	log.Debug("[seaweedfs] readFile", log.String("name", name))

	f, err := s.Open(name)
	if err != nil {
		return nil, err
	}
	defer func(f gofs.File) {
		if err := f.Close(); err != nil {
			log.Error("[seaweedfs] readFile", log.Err(err))
		}
	}(f)

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "readFile", Path: name, Err: err})
	}
	return b, nil
}

// Remove ...
func (s *SeaweedFS) Remove(name string) error {
	log.Debug("[seaweedfs] remove", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := remove(ctx, s, name); err != nil {
		return fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "remove", Path: name, Err: err})
	}
	return nil
}

// RemoveAll ...
func (s *SeaweedFS) RemoveAll(path string) error {
	log.Debug("[seaweedfs] removeAll", log.String("path", path))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := removeAll(ctx, s, path); err != nil {
		return fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "rename", Path: path, Err: err})
	}
	return nil
}

// Rename ...
func (s *SeaweedFS) Rename(oldpath string, newpath string) error {
	log.Debug("[seaweedfs] rename",
		log.String("old_path", oldpath),
		log.String("new_path", newpath),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rename(ctx, s, oldpath, newpath); err != nil {
		return fmt.Errorf("seaweedfs: %w", &goos.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err})
	}
	return nil
}

// Root ...
func (s *SeaweedFS) Root() (string, error) {
	return s.cluster.Filer().Root().Path().Name(), nil
}

// Stat ...
func (s *SeaweedFS) Stat(name string) (gofs.FileInfo, error) {
	log.Debug("[seaweedfs] stat", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fe, err := stat(ctx, s, name)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "stat", Path: name, Err: err})
	}

	e, err := FSEntry(s, fe)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "stat", Path: name, Err: err})
	}
	return e, nil
}

// Sub ...
func (s *SeaweedFS) Sub(dir string) (gofs.FS, error) {
	log.Debug("[seaweedfs] sub", log.String("current", s.entry.Name()), log.String("dir", dir))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := sub(ctx, s, dir)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "sub", Path: dir, Err: err})
	}
	return sub, nil
}

// WriteFile ...
func (s *SeaweedFS) WriteFile(name string, data []byte, mode gofs.FileMode) error {
	log.Debug("[seaweedfs] writeFile",
		log.String("name", name),
		log.Int("content_length", len(data)),
		log.String("mode", mode.String()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := open(ctx, s, name, fs.O_RDWR|fs.O_CREATE|fs.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("seaweedfs: %w", &gofs.PathError{Op: "writeFile", Path: name, Err: err})
	}
	defer func(f *File) {
		if err := f.Close(); err != nil {
			log.Error("[seaweedfs] writeFile", log.Err(err))
		}
	}(f)

	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// String returns a string representation of SeaweedFS.
func (s *SeaweedFS) String() string {
	m := make(map[string]any)
	if s.entry != nil {
		e, err := s.entry.ToMap()
		if err != nil {
			m["file"] = err.Error()
		}
		m["file"] = e
	}
	m["root"] = s.cluster.Filer().Root().Entry()
	return string(anchor.ToJSONFormatted(m))
}

func (s *SeaweedFS) path() (string, error) {
	r, err := s.Root()
	if err != nil {
		return "", err
	}

	// TODO: This needs to be cleaned up...
	p := s.entry.Path().String()
	if strings.HasPrefix(p, s.PathSeparator()) {
		p = strings.TrimPrefix(p, s.PathSeparator())
	}

	if p == r {
		return p, nil
	}

	if s.entry != nil && strings.HasPrefix(p, r) {
		p = strings.TrimPrefix(p, r)
		if strings.HasPrefix(p, s.PathSeparator()) {
			p = strings.TrimPrefix(p, s.PathSeparator())
		}
		return p, nil
	}
	return "", errors.New("seaweedfs: path not found")
}

func create(ctx context.Context, weed *SeaweedFS, name string, flag int, mode gofs.FileMode) (*File, error) {
	if mode&gofs.ModeDir != 0 {
		log.Trace("[seaweedfs] directory mode bits set, creating path as directory", log.String("name", name))

		dir, err := mkdirAll(ctx, weed, name, mode)
		if err != nil {
			return nil, err
		}

		file, err := newFile(dir, flag)
		if err != nil {
			return nil, err
		}
		return file, nil
	}

	p, err := fs.SplitPath(weed, name)
	if err != nil {
		return nil, err
	}

	if len(p) == 1 {
		dir, err := mkdirAll(ctx, weed, filepath.Dir(name), mode)
		if err != nil {
			return nil, err
		}
		weed = dir
	}

	e, err := weed.cluster.Filer().Create(ctx, name, mode)
	if err != nil {
		return nil, err
	}
	return newFile(weed, flag, WithEntry(e))
}

func mkdir(ctx context.Context, weed *SeaweedFS, name string, mode gofs.FileMode) (*SeaweedFS, error) {
	n, err := fs.CleanPath(weed, name)
	if err != nil {
		return nil, err
	}

	if fs.EndsWithDot(weed, n) {
		return nil, gofs.ErrInvalid
	}

	p, err := fs.SplitPath(weed, n)
	if err != nil {
		return nil, err
	}

	if len(p) > 1 {
		return nil, gofs.ErrInvalid
	}

	if _, err := stat(ctx, weed, n); err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return nil, err
		}
	} else {
		return nil, gofs.ErrExist
	}

	dir := filepath.Join(weed.entry.Path().String(), n)

	log.Trace("[seaweedfs] mkdir: creating directory",
		log.String("name", n),
		log.String("parent", weed.entry.Path().String()),
		log.Int("mode", int(mode)))

	e, err := weed.cluster.Filer().Create(ctx, dir, mode|gofs.ModeDir)
	if err != nil {
		return nil, err
	}
	return &SeaweedFS{
		cluster: weed.cluster,
		entry:   e,
		gid:     weed.gid,
		uid:     weed.uid,
	}, nil
}

func mkdirAll(ctx context.Context, weed *SeaweedFS, path string, mode gofs.FileMode) (*SeaweedFS, error) {
	p, err := fs.SplitPath(weed, path)
	if err != nil {
		return nil, err
	}

	for _, dir := range p {
		s, err := sub(ctx, weed, dir)
		if err != nil {
			if !errors.Is(err, gofs.ErrNotExist) {
				return nil, err
			}
		}

		if s != nil {
			weed = s
			continue
		}

		if weed, err = mkdir(ctx, weed, dir, mode); err != nil {
			return nil, err
		}
	}
	return weed, nil
}

func open(ctx context.Context, weed *SeaweedFS, name string, flag int, mode gofs.FileMode) (*File, error) {
	log.Trace("[seaweedfs] open",
		log.String("name", name),
		log.Int("flag", flag),
		log.String("mode", mode.String()))

	name, err := fs.CleanPath(weed, name)
	if err != nil {
		return nil, err
	}

	if name == "." {
		return newFile(weed, fs.O_RDONLY)
	}

	e, err := stat(ctx, weed, name)
	if err != nil {
		if errors.Is(err, gofs.ErrNotExist) && flag&fs.O_CREATE != 0 {
			log.Trace("[seaweedfs] creating new file", log.String("name", name))
			return create(ctx, weed, name, flag, mode)
		}
		return nil, err
	}

	if !e.IsDir() {
		return newFile(weed, flag, WithEntry(e))
	}
	return newFile(weed, fs.O_RDONLY, WithEntry(e))
}

func remove(ctx context.Context, weed *SeaweedFS, name string) error {
	fi, err := stat(ctx, weed, name)
	if err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return err
		}
		return nil
	}

	if fi.IsDir() {
		return fs.ErrIsDir
	}

	if _, err := weed.cluster.Filer().Remove(ctx, name); err != nil {
		return err
	}
	return nil
}

func removeAll(ctx context.Context, weed *SeaweedFS, path string) error {
	e, err := stat(ctx, weed, path)
	if err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return err
		}
		return nil
	}

	r, err := weed.Root()
	if err != nil {
		return err
	}

	if e.Name() == r {
		return gofs.ErrInvalid
	}

	if _, err := weed.cluster.Filer().Remove(ctx, path); err != nil {
		return err
	}
	return nil
}

func rename(ctx context.Context, weed *SeaweedFS, oldpath string, newpath string) error {
	o, err := fs.CleanPath(weed, oldpath)
	if err != nil {
		return err
	}

	n, err := fs.CleanPath(weed, newpath)
	if err != nil {
		return err
	}

	if fs.EndsWithDot(weed, o) || fs.EndsWithDot(weed, n) {
		return gofs.ErrInvalid
	}
	return weed.cluster.Filer().Rename(ctx, o, n)
}

func stat(ctx context.Context, weed *SeaweedFS, name string) (*filer.Entry, error) {
	name, err := fs.CleanPath(weed, name)
	if err != nil {
		return nil, err
	}

	if name == "." {
		return weed.entry, nil
	}

	r, err := weed.Root()
	if err != nil {
		return nil, err
	}

	if weed.entry.Name() != r {
		name = strings.Join([]string{weed.entry.Name(), name}, weed.PathSeparator())
	}
	return weed.cluster.Filer().Stat(ctx, name)
}

func sub(ctx context.Context, weed *SeaweedFS, dir string) (*SeaweedFS, error) {
	e, err := stat(ctx, weed, dir)
	if err != nil {
		return nil, err
	}

	if !e.IsDir() {
		return nil, fs.ErrNotDir
	}
	return &SeaweedFS{
		cluster:    weed.cluster,
		entry:      e,
		gid:        weed.gid,
		httpClient: weed.httpClient,
		uid:        weed.uid,
	}, nil
}

// FSEntry converts a filer.Entry to an fs.Entry.
func FSEntry(fsys fs.FS, filerEntry *filer.Entry, options ...func(*fs.Entry)) (*fs.Entry, error) {
	if fsys == nil {
		return nil, errors.New("file system is required")
	}

	attrs := []func(*fs.Attribute){fs.WithSize(uint64(filerEntry.Size()))}
	if pbAttrs := filerEntry.PB().GetAttributes(); pbAttrs != nil {
		attrs = append(attrs,
			fs.WithCtime(time.Unix(pbAttrs.GetCrtime(), 0)),
			fs.WithGID(pbAttrs.GetGid()),
			fs.WithInode(pbAttrs.GetInode()),
			fs.WithMode(pbAttrs.GetFileMode()),
			fs.WithMtime(time.Unix(pbAttrs.GetMtime(), 0)),
			fs.WithOwner(pbAttrs.GetUserName()),
			fs.WithUID(pbAttrs.GetUid()))

		if !filerEntry.IsDir() {
			attrs = append(attrs, fs.WithMimeType(pbAttrs.GetMime()))
		}
	}

	entryAttrs, err := fs.NewAttributes(attrs...)
	if err != nil {
		return nil, err
	}

	fsEntry, err := fs.NewEntry(fsPath(fsys, filerEntry.Path()), append(options, fs.WithAttributes(entryAttrs))...)
	if err != nil {
		return nil, err
	}
	return fsEntry, nil
}

func fsPath(fsys fs.FS, path filer.Path) string {
	p := strings.TrimPrefix(path.String(), path.Root())
	if p == fsys.PathSeparator() || p == "" {
		return "."
	}

	if p[0] == '/' {
		p = strings.TrimPrefix(p, `/`)
	}
	return p
}
