package filer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log"
	"github.com/transientvariable/support"

	"google.golang.org/grpc/status"
)

// Update ...
func (f *Filer) Update(ctx context.Context, entry *Entry) error {
	if entry == nil {
		return &client.Error{Op: "update", Client: f, Err: errors.New("entry is required for update")}
	}

	if entry.PB() == nil {
		return &client.Error{Op: "update", Client: f, Err: errors.New("protobuf entry is required for update")}
	}

	if _, err := f.Stat(ctx, entry.Path().String()); err != nil {
		return err
	}

	log.Trace("[filer] update", log.String("name", entry.Name()), log.String("path", entry.Path().String()))

	fe := entry.PB()
	if fe.GetAttributes() == nil {
		return &client.Error{Op: "update", Client: f, Err: errors.New("protobuf entry attributes missing")}
	}
	fe.GetAttributes().Mtime = time.Now().Unix()

	req := &filer_pb.UpdateEntryRequest{
		Directory:  entry.Path().Dir(),
		Entry:      fe,
		Signatures: []int32{f.signature},
	}

	log.Trace(fmt.Sprintf("[filer] update request: \n%s", support.ToJSONFormatted(req)))

	resp, err := f.PB().UpdateEntry(ctx, req)
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return &client.Error{Op: "update", Client: f, Err: errors.New(s.Message())}
		}
		return &client.Error{Op: "update", Client: f, Err: err}
	}

	if resp.String() != "" {
		log.Trace(fmt.Sprintf("[filer] update response: %s", resp.String()))
	}
	return nil
}
