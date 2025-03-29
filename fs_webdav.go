package lettuce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/transientvariable/fs-go"
	"github.com/transientvariable/log-go"

	"golang.org/x/net/webdav"

	gofs "io/fs"
)

var (
	_ webdav.FileSystem = (*WebDAV)(nil)
)

// WebDAV an implementation of the webdav.FileSystem using SeaweedFS for the storage backend.
type WebDAV struct {
	closed bool
	mutex  sync.Mutex
	weed   *SeaweedFS
}

// NewWebDAV creates a new webdav.FileSystem backed by the provided SeaweedFS instance.
func NewWebDAV(weed *SeaweedFS) (*WebDAV, error) {
	if weed == nil {
		return nil, errors.New("seaweedfs_webdav: seaweedfs backend is required")
	}
	return &WebDAV{weed: weed}, nil
}

// Close releases any resources used by WebDAV.
func (w *WebDAV) Close() error {
	if w == nil {
		return gofs.ErrInvalid
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.closed {
		w.closed = true
		if w.weed != nil {
			if err := w.weed.Close(); err != nil && !errors.Is(err, gofs.ErrClosed) {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("seaweedfs_webdav: %w", gofs.ErrClosed)
}

func (w *WebDAV) Mkdir(ctx context.Context, name string, mode os.FileMode) error {
	name = resolve(name)

	log.Debug("[seaweedfs:webdav] mkdir", log.String("name", name), log.String("perm", mode.String()))

	if !w.isDir(ctx, filepath.Dir(name), "mkdir") {
		return gofs.ErrNotExist
	}

	if _, err := w.stat(ctx, name, "mkdir"); err == nil {
		return gofs.ErrExist
	}

	if _, err := mkdirAll(ctx, w.weed, name, mode); err != nil {
		return err
	}
	return nil
}

func (w *WebDAV) OpenFile(ctx context.Context, name string, flag int, mode os.FileMode) (webdav.File, error) {
	log.Debug("[seaweedfs:webdav] openFile",
		log.String("name", name),
		log.Int("flag", flag),
		log.String("perm", mode.String()))

	name = resolve(name)
	if !w.isDir(ctx, filepath.Dir(name), "openFile") {
		return nil, gofs.ErrNotExist
	}

	f, err := open(ctx, w.weed, name, flag, mode)
	if err != nil {
		if errors.Is(err, gofs.ErrNotExist) {
			return nil, gofs.ErrNotExist
		}
		return nil, err
	}
	return f, nil
}

func (w *WebDAV) Rename(ctx context.Context, oldName string, newName string) error {
	log.Debug("[seaweedfs:webdav] rename", log.String("old_name", oldName), log.String("new_name", newName))

	if err := rename(ctx, w.weed, resolve(oldName), resolve(newName)); err != nil {
		return err
	}
	return nil
}

func (w *WebDAV) RemoveAll(ctx context.Context, name string) error {
	log.Debug("[seaweedfs:webdav] removeAll", log.String("name", name))

	if err := removeAll(ctx, w.weed, resolve(name)); err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (w *WebDAV) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	log.Debug("[seaweedfs:webdav] stat", log.String("name", name))

	e, err := w.stat(ctx, resolve(name), "stat")
	if err != nil {
		if errors.Is(err, gofs.ErrNotExist) {
			return e, gofs.ErrNotExist
		}
	}
	return e, err
}

func (w *WebDAV) isDir(ctx context.Context, name string, op string) bool {
	fi, err := w.stat(ctx, name, op+"_isDir")
	if err != nil {
		return false
	}
	return fi.IsDir()
}

func (w *WebDAV) stat(ctx context.Context, name string, op string) (*fs.Entry, error) {
	log.Debug("[seaweedfs:webdav] stat", log.String("name", name), log.String("op", op))

	fe, err := stat(ctx, w.weed, name)
	if err != nil {
		return nil, err
	}

	e, err := FSEntry(w.weed, fe)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func resolve(name string) string {
	name = path.Clean(name)
	if name = strings.TrimPrefix(name, `/`); name == "" {
		return `.`
	}
	return name
}
