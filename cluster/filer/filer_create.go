package filer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log-go"
	"github.com/transientvariable/support-go"

	"google.golang.org/grpc/status"

	gofs "io/fs"
)

// Create creates a new Filer entry.
//
// If the operation is successful, an Entry will be returned representing the created entry.
func (f *Filer) Create(ctx context.Context, name string, mode gofs.FileMode) (*Entry, error) {
	e, err := f.Stat(ctx, name)
	if err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return nil, err
		}
	}

	if e != nil {
		return e, &client.Error{Op: "create", Client: f, Err: gofs.ErrExist}
	}

	path, err := f.path(name)
	if err != nil {
		return nil, &client.Error{Op: "create", Client: f, Err: err}
	}

	log.Trace("[filer] create",
		log.Bool("is_dir", mode.IsDir()),
		log.Int("mode", int(mode)),
		log.String("name", name),
		log.String("path", path.String()))

	attrs := &filer_pb.FuseAttributes{
		Mtime:    time.Now().Unix(),
		Crtime:   time.Now().Unix(),
		FileMode: uint32(mode),
		Gid:      uint32(f.root.entry.GID()),
		Uid:      uint32(f.root.entry.UID()),
	}

	pbEntry := &filer_pb.Entry{
		Name:        path.Name(),
		IsDirectory: mode&gofs.ModeDir != 0,
		Attributes:  attrs,
	}

	req := &filer_pb.CreateEntryRequest{
		Directory:  path.Dir(),
		Entry:      pbEntry,
		Signatures: []int32{f.signature},
	}

	log.Trace(fmt.Sprintf("[filer] create request: \n%s", support.ToJSONFormatted(req)))

	resp, err := f.PB().CreateEntry(ctx, req)
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return nil, &client.Error{Op: "create", Client: f, Err: errors.New(s.Message())}
		}
		return nil, &client.Error{Op: "create", Client: f, Err: err}
	}

	log.Trace(fmt.Sprintf("[filer] create response: %s", resp.String()))

	if respErr := resp.GetError(); respErr != "" {
		return nil, &client.Error{Op: "create", Client: f, Err: errors.New(respErr)}
	}
	return f.NewEntry(filepath.Dir(name), pbEntry)
}
