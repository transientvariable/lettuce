package filer

import (
	"context"
	"errors"
	"fmt"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log-go"
	"github.com/transientvariable/support-go"

	"google.golang.org/grpc/status"
)

// Remove ...
func (f *Filer) Remove(ctx context.Context, name string) (*Entry, error) {
	e, err := f.Stat(ctx, name)
	if err != nil {
		return e, err
	}

	log.Trace("[filer] remove", log.String("name", name), log.String("path", e.Path().String()))

	req := &filer_pb.DeleteEntryRequest{
		Directory:          e.Path().Dir(),
		Name:               e.Path().Name(),
		IsDeleteData:       true,
		IsFromOtherCluster: false,
		Signatures:         []int32{f.signature},
	}

	if e.IsDir() {
		req.IgnoreRecursiveError = false
		req.IsRecursive = true
	}

	log.Trace(fmt.Sprintf("[filer] remove request: %s", support.ToJSONFormatted(req)))

	resp, err := f.PB().DeleteEntry(ctx, req)
	if err != nil {
		s, ok := status.FromError(err)
		if !ok {
			return nil, &client.Error{Op: "remove", Client: f, Err: err}
		}
		return nil, &client.Error{Op: "remove", Client: f, Err: errors.New(s.Message())}
	}

	log.Trace(fmt.Sprintf("[filer] remove response: %s", resp.String()))

	if respErr := resp.GetError(); respErr != "" {
		return e, &client.Error{Op: "remove", Client: f, Err: errors.New(respErr)}
	}
	return e, nil
}
