package filer

import (
	"context"
	"errors"
	"fmt"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log-go"

	"google.golang.org/grpc/status"

	gofs "io/fs"
)

// Rename renames (moves) oldpath to newpath.
//
// If newpath already exists and is not a directory, Rename replaces it.
func (f *Filer) Rename(ctx context.Context, oldpath string, newpath string) error {
	if _, err := f.Stat(ctx, oldpath); err != nil {
		return err
	}

	nfi, err := f.Stat(ctx, newpath)
	if err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return &client.Error{Op: "rename", Client: f, Err: err}
		}
	} else if nfi.IsDir() {
		return &client.Error{Op: "rename", Client: f, Err: fmt.Errorf("%s: %w", newpath, gofs.ErrExist)}
	}

	log.Trace("[filer] rename", log.String("old_path", oldpath), log.String("new_path", newpath))

	op, err := f.path(oldpath)
	if err != nil {
		return &client.Error{Op: "rename", Client: f, Err: err}
	}

	np, err := f.path(newpath)
	if err != nil {
		return &client.Error{Op: "rename", Client: f, Err: err}
	}

	req := &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: op.Dir(),
		OldName:      op.Name(),
		NewDirectory: np.Dir(),
		NewName:      np.Name(),
	}

	log.Trace(fmt.Sprintf("[filer] rename request: \n%s", anchor.ToJSONFormatted(req)))

	resp, err := f.PB().AtomicRenameEntry(ctx, req)
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return &client.Error{Op: "rename", Client: f, Err: errors.New(s.Message())}
		}
		return &client.Error{Op: "rename", Client: f, Err: err}
	}

	log.Trace(fmt.Sprintf("[filer] rename response: %s", resp.String()))
	return nil
}
