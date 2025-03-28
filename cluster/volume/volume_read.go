package volume

import (
	"context"
	"errors"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/volume_server_pb"

	"google.golang.org/grpc/status"
)

func (v *Volume) Read(ctx context.Context) ([]byte, error) {
	//volume_server_pb.ReadNeedleMetaRequest{}

	req := &volume_server_pb.ReadNeedleBlobRequest{}

	resp, err := v.PB().ReadNeedleBlob(ctx, req)
	if err != nil {
		s, ok := status.FromError(err)
		if !ok {
			return nil, &client.Error{Op: "delete", Client: v, Err: err}
		}
		return nil, &client.Error{Op: "delete", Client: v, Err: errors.New(s.Message())}
	}

	resp.GetNeedleBlob()
	return nil, nil
}
