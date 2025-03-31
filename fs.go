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
	_ fs.FS = (*Lettuce)(nil)
)

// Lettuce is a file system provider that implements fs.FS using SeaweedFS for the storage backend.
type Lettuce struct {
	closed     bool
	cluster    *cluster.Cluster
	entry      *filer.Entry
	gid        int32
	httpClient *gohttp.Client
	mutex      sync.Mutex
	uid        int32
}

// New creates a new fs.FS backed by SeaweedFS using the provided options.
func New(options ...func(*Lettuce)) (*Lettuce, error) {
	let := &Lettuce{httpClient: http.DefaultClient()}
	for _, opt := range options {
		opt(let)
	}

	if let.cluster == nil {
		log.Warn("[lettuce] cluster configuration not provided, using default")

		c, err := cluster.New()
		if err != nil {
			return nil, fmt.Errorf("lettuce: %w", err)
		}
		let.cluster = c
	}
	let.entry = let.cluster.Filer().Root().Entry()

	if let.gid <= 0 {
		let.gid = client.GID
	}

	if let.uid <= 0 {
		let.uid = client.UID
	}
	return let, nil
}

// Close releases any resources used by Lettuce.
func (l *Lettuce) Close() error {
	log.Debug("[lettuce] close")

	if l == nil {
		return gofs.ErrInvalid
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.entry != nil && l.entry.Name() != l.PathSeparator() {
		return nil
	}

	if !l.closed {
		l.closed = true
		if l.cluster != nil {
			if err := l.cluster.Close(); err != nil && !errors.Is(err, gofs.ErrClosed) {
				return err
			}
		}
		return l.cluster.Close()
	}
	return fmt.Errorf("lettuce: %w", gofs.ErrClosed)
}

// Cluster returns the cluster API used for performing operations against a SeaweedFS backend.
func (l *Lettuce) Cluster() *cluster.Cluster {
	// TODO: Check if FS is closed?
	return l.cluster
}

// Create ...
func (l *Lettuce) Create(name string) (fs.File, error) {
	log.Debug("[lettuce] create", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := open(ctx, l, name, fs.O_RDWR|fs.O_CREATE|fs.O_TRUNC, modeCreate)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "create", Path: name, Err: err})
	}
	return f, nil
}

// Glob ...
func (l *Lettuce) Glob(pattern string) ([]string, error) {
	log.Debug("[lettuce] glob", log.String("pattern", pattern))

	var matches []string
	err := gofs.WalkDir(l, `.`, func(path string, entry gofs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if fs.EndsWithDot(l, path) {
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
			return matches, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "glob", Err: err})
		}
		return matches, err
	}
	return matches, nil
}

// Mkdir creates a new directory with the specified name and permission bits.
func (l *Lettuce) Mkdir(name string, perm gofs.FileMode) error {
	log.Debug("[lettuce] mkdir", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := mkdir(ctx, l, name, perm); err != nil {
		return fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "mkdir", Path: name, Err: err})
	}
	return nil
}

// MkdirAll ...
func (l *Lettuce) MkdirAll(path string, mode gofs.FileMode) error {
	log.Debug("[lettuce] mkdirAll",
		log.String("path", path),
		log.String("mode", mode.String()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := mkdirAll(ctx, l, path, mode); err != nil {
		return fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "mkdirAll", Path: path, Err: err})
	}
	return nil
}

// Open ...
func (l *Lettuce) Open(name string) (gofs.File, error) {
	log.Debug("[lettuce] open", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := open(ctx, l, name, fs.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "open", Path: name, Err: err})
	}
	return f, nil
}

// OpenFile ...
func (l *Lettuce) OpenFile(name string, flag int, mode gofs.FileMode) (fs.File, error) {
	log.Debug("[lettuce] openFile",
		log.String("name", name),
		log.Int("flag", flag),
		log.String("mode", mode.String()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := open(ctx, l, name, flag, mode)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "openFile", Path: name, Err: err})
	}
	return f, nil
}

// PathSeparator ...
func (l *Lettuce) PathSeparator() string {
	return l.cluster.Filer().PathSeparator()
}

// Provider ...
func (l *Lettuce) Provider() string {
	return "lettuce"
}

// ReadDir ...
func (l *Lettuce) ReadDir(name string) ([]gofs.DirEntry, error) {
	log.Debug("[lettuce] readDir", log.String("current_dir", l.entry.Name()), log.String("name", name))

	sub, err := l.Sub(name)
	if err != nil {
		return nil, err
	}
	dir := sub.(*Lettuce)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	list, err := newDirIterator(ctx, l, dir.entry)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "readDir", Path: dir.entry.Name(), Err: err})
	}

	de, err := list.NextN(-1)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "readDir", Path: dir.entry.Name(), Err: err})
	}

	entries := make([]gofs.DirEntry, len(de))
	for i, e := range de {
		entries[i] = e
	}
	return entries, nil
}

// ReadFile ...
func (l *Lettuce) ReadFile(name string) ([]byte, error) {
	log.Debug("[lettuce] readFile", log.String("name", name))

	f, err := l.Open(name)
	if err != nil {
		return nil, err
	}
	defer func(f gofs.File) {
		if err := f.Close(); err != nil {
			log.Error("[lettuce] readFile", log.Err(err))
		}
	}(f)

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "readFile", Path: name, Err: err})
	}
	return b, nil
}

// Remove ...
func (l *Lettuce) Remove(name string) error {
	log.Debug("[lettuce] remove", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := remove(ctx, l, name); err != nil {
		return fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "remove", Path: name, Err: err})
	}
	return nil
}

// RemoveAll ...
func (l *Lettuce) RemoveAll(path string) error {
	log.Debug("[lettuce] removeAll", log.String("path", path))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := removeAll(ctx, l, path); err != nil {
		return fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "rename", Path: path, Err: err})
	}
	return nil
}

// Rename ...
func (l *Lettuce) Rename(oldpath string, newpath string) error {
	log.Debug("[lettuce] rename",
		log.String("old_path", oldpath),
		log.String("new_path", newpath),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rename(ctx, l, oldpath, newpath); err != nil {
		return fmt.Errorf("lettuce: %w", &goos.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err})
	}
	return nil
}

// Root ...
func (l *Lettuce) Root() (string, error) {
	return l.cluster.Filer().Root().Path().Name(), nil
}

// Stat ...
func (l *Lettuce) Stat(name string) (gofs.FileInfo, error) {
	log.Debug("[lettuce] stat", log.String("name", name))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fe, err := stat(ctx, l, name)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "stat", Path: name, Err: err})
	}

	e, err := FSEntry(l, fe)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "stat", Path: name, Err: err})
	}
	return e, nil
}

// Sub ...
func (l *Lettuce) Sub(dir string) (gofs.FS, error) {
	log.Debug("[lettuce] sub", log.String("current", l.entry.Name()), log.String("dir", dir))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := sub(ctx, l, dir)
	if err != nil {
		return nil, fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "sub", Path: dir, Err: err})
	}
	return sub, nil
}

// WriteFile ...
func (l *Lettuce) WriteFile(name string, data []byte, mode gofs.FileMode) error {
	log.Debug("[lettuce] writeFile",
		log.String("name", name),
		log.Int("content_length", len(data)),
		log.String("mode", mode.String()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := open(ctx, l, name, fs.O_RDWR|fs.O_CREATE|fs.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("lettuce: %w", &gofs.PathError{Op: "writeFile", Path: name, Err: err})
	}
	defer func(f *File) {
		if err := f.Close(); err != nil {
			log.Error("[lettuce] writeFile", log.Err(err))
		}
	}(f)

	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// String returns a string representation of Lettuce.
func (l *Lettuce) String() string {
	m := make(map[string]any)
	if l.entry != nil {
		e, err := l.entry.ToMap()
		if err != nil {
			m["file"] = err.Error()
		}
		m["file"] = e
	}
	m["root"] = l.cluster.Filer().Root().Entry()
	return string(anchor.ToJSONFormatted(m))
}

func (l *Lettuce) path() (string, error) {
	r, err := l.Root()
	if err != nil {
		return "", err
	}

	// TODO: This needs to be cleaned up...
	p := l.entry.Path().String()
	if strings.HasPrefix(p, l.PathSeparator()) {
		p = strings.TrimPrefix(p, l.PathSeparator())
	}

	if p == r {
		return p, nil
	}

	if l.entry != nil && strings.HasPrefix(p, r) {
		p = strings.TrimPrefix(p, r)
		if strings.HasPrefix(p, l.PathSeparator()) {
			p = strings.TrimPrefix(p, l.PathSeparator())
		}
		return p, nil
	}
	return "", errors.New("lettuce: path not found")
}

func create(ctx context.Context, let *Lettuce, name string, flag int, mode gofs.FileMode) (*File, error) {
	if mode&gofs.ModeDir != 0 {
		log.Trace("[lettuce] directory mode bits set, creating path as directory", log.String("name", name))

		dir, err := mkdirAll(ctx, let, name, mode)
		if err != nil {
			return nil, err
		}

		file, err := newFile(dir, flag)
		if err != nil {
			return nil, err
		}
		return file, nil
	}

	p, err := fs.SplitPath(let, name)
	if err != nil {
		return nil, err
	}

	if len(p) == 1 {
		dir, err := mkdirAll(ctx, let, filepath.Dir(name), mode)
		if err != nil {
			return nil, err
		}
		let = dir
	}

	e, err := let.cluster.Filer().Create(ctx, name, mode)
	if err != nil {
		return nil, err
	}
	return newFile(let, flag, WithEntry(e))
}

func mkdir(ctx context.Context, let *Lettuce, name string, mode gofs.FileMode) (*Lettuce, error) {
	n, err := fs.CleanPath(let, name)
	if err != nil {
		return nil, err
	}

	if fs.EndsWithDot(let, n) {
		return nil, gofs.ErrInvalid
	}

	p, err := fs.SplitPath(let, n)
	if err != nil {
		return nil, err
	}

	if len(p) > 1 {
		return nil, gofs.ErrInvalid
	}

	if _, err := stat(ctx, let, n); err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return nil, err
		}
	} else {
		return nil, gofs.ErrExist
	}

	dir := filepath.Join(let.entry.Path().String(), n)

	log.Trace("[lettuce] mkdir: creating directory",
		log.String("name", n),
		log.String("parent", let.entry.Path().String()),
		log.Int("mode", int(mode)))

	e, err := let.cluster.Filer().Create(ctx, dir, mode|gofs.ModeDir)
	if err != nil {
		return nil, err
	}
	return &Lettuce{
		cluster: let.cluster,
		entry:   e,
		gid:     let.gid,
		uid:     let.uid,
	}, nil
}

func mkdirAll(ctx context.Context, let *Lettuce, path string, mode gofs.FileMode) (*Lettuce, error) {
	p, err := fs.SplitPath(let, path)
	if err != nil {
		return nil, err
	}

	for _, dir := range p {
		s, err := sub(ctx, let, dir)
		if err != nil {
			if !errors.Is(err, gofs.ErrNotExist) {
				return nil, err
			}
		}

		if s != nil {
			let = s
			continue
		}

		if let, err = mkdir(ctx, let, dir, mode); err != nil {
			return nil, err
		}
	}
	return let, nil
}

func open(ctx context.Context, let *Lettuce, name string, flag int, mode gofs.FileMode) (*File, error) {
	log.Trace("[lettuce] open",
		log.String("name", name),
		log.Int("flag", flag),
		log.String("mode", mode.String()))

	name, err := fs.CleanPath(let, name)
	if err != nil {
		return nil, err
	}

	if name == "." {
		return newFile(let, fs.O_RDONLY)
	}

	e, err := stat(ctx, let, name)
	if err != nil {
		if errors.Is(err, gofs.ErrNotExist) && flag&fs.O_CREATE != 0 {
			log.Trace("[lettuce] creating new file", log.String("name", name))
			return create(ctx, let, name, flag, mode)
		}
		return nil, err
	}

	if !e.IsDir() {
		return newFile(let, flag, WithEntry(e))
	}
	return newFile(let, fs.O_RDONLY, WithEntry(e))
}

func remove(ctx context.Context, let *Lettuce, name string) error {
	fi, err := stat(ctx, let, name)
	if err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return err
		}
		return nil
	}

	if fi.IsDir() {
		return fs.ErrIsDir
	}

	if _, err := let.cluster.Filer().Remove(ctx, name); err != nil {
		return err
	}
	return nil
}

func removeAll(ctx context.Context, let *Lettuce, path string) error {
	e, err := stat(ctx, let, path)
	if err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return err
		}
		return nil
	}

	r, err := let.Root()
	if err != nil {
		return err
	}

	if e.Name() == r {
		return gofs.ErrInvalid
	}

	if _, err := let.cluster.Filer().Remove(ctx, path); err != nil {
		return err
	}
	return nil
}

func rename(ctx context.Context, let *Lettuce, oldpath string, newpath string) error {
	o, err := fs.CleanPath(let, oldpath)
	if err != nil {
		return err
	}

	n, err := fs.CleanPath(let, newpath)
	if err != nil {
		return err
	}

	if fs.EndsWithDot(let, o) || fs.EndsWithDot(let, n) {
		return gofs.ErrInvalid
	}
	return let.cluster.Filer().Rename(ctx, o, n)
}

func stat(ctx context.Context, let *Lettuce, name string) (*filer.Entry, error) {
	name, err := fs.CleanPath(let, name)
	if err != nil {
		return nil, err
	}

	if name == "." {
		return let.entry, nil
	}

	r, err := let.Root()
	if err != nil {
		return nil, err
	}

	if let.entry.Name() != r {
		name = strings.Join([]string{let.entry.Name(), name}, let.PathSeparator())
	}
	return let.cluster.Filer().Stat(ctx, name)
}

func sub(ctx context.Context, let *Lettuce, dir string) (*Lettuce, error) {
	e, err := stat(ctx, let, dir)
	if err != nil {
		return nil, err
	}

	if !e.IsDir() {
		return nil, fs.ErrNotDir
	}
	return &Lettuce{
		cluster:    let.cluster,
		entry:      e,
		gid:        let.gid,
		httpClient: let.httpClient,
		uid:        let.uid,
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
