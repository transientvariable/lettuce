package filer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log-go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gofs "io/fs"
)

// Stat returns the Entry describing the named file or directory which can be used in cases where fs.FileInfo is
// required.
func (f *Filer) Stat(ctx context.Context, name string) (*Entry, error) {
	if name == f.root.Entry().Name() {
		return f.Root().Entry(), nil
	}

	path, err := f.path(name)
	if err != nil {
		return nil, &client.Error{Op: "stat", Client: f, Err: err}
	}

	log.Trace("[filer] stat", log.String("name", name), log.String("path", path.String()))

	resp, err := f.PB().LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
		Directory: path.Dir(),
		Name:      path.Name(),
	})
	if err != nil {
		s, ok := status.FromError(err)
		if !ok {
			return nil, &client.Error{Op: "stat", Client: f, Err: err}
		}

		switch s.Code() {
		case codes.NotFound:
			return nil, &client.Error{Op: "stat", Client: f, Err: fmt.Errorf("%s: %w", name, gofs.ErrNotExist)}
		case codes.Unknown:
			if strings.Contains(s.Message(), errNotFoundStr) {
				return nil, &client.Error{Op: "stat", Client: f, Err: fmt.Errorf("%s: %w", name, gofs.ErrNotExist)}
			}
		default:
			return nil, &client.Error{Op: "stat", Client: f, Err: errors.New(s.Message())}
		}
	}

	if fe := resp.GetEntry(); fe != nil {
		e, err := f.NewEntry(filepath.Dir(name), fe)
		if err != nil {
			return e, err
		}
		return e, nil
	}
	return nil, &client.Error{Op: "stat", Client: f, Err: fmt.Errorf("%s: %w", name, gofs.ErrNotExist)}
}
