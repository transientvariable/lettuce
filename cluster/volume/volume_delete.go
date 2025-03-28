package volume

import (
	"context"
	"errors"
	"fmt"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/volume_server_pb"
	"github.com/transientvariable/log-go"
	"github.com/transientvariable/support-go"

	"google.golang.org/grpc/status"
)

// DeleteResult represents the result of deleting one or more file IDs from a single Volume.
type DeleteResult struct {
	Needles []Needle `json:"needles,omitempty"`
	Volume  string   `json:"volume,omitempty"`
}

// String returns a string representation of the DeleteResult.
func (r DeleteResult) String() string {
	return string(support.ToJSONFormatted(r))
}

// Delete deletes the set of file IDs from the Volume.
//
// Errors that occur during handling of requests will be returned to the caller.
// When processing the response of a successful deletion request, errors of type ErrNotFound will be recorded for each
// file ID and can be asserted with Needle.Err. All other errors encountered while processing file IDs from the response
// will immediately return with a non-nil error to the caller.
func (v *Volume) Delete(ctx context.Context, fileIDs ...string) (DeleteResult, error) {
	log.Trace("[volume] deleting file ID(s)",
		log.Int("total", len(fileIDs)),
		log.String("volume", v.ID().Host()))

	req := &volume_server_pb.BatchDeleteRequest{
		FileIds:         fileIDs,
		SkipCookieCheck: true,
	}

	log.Trace(fmt.Sprintf("[volume] delete request: \n%s", support.ToJSONFormatted(req)))

	dr := DeleteResult{Volume: v.ID().Host()}
	resp, err := v.PB().BatchDelete(ctx, req)
	if err != nil {
		s, ok := status.FromError(err)
		if !ok {
			return dr, &client.Error{Op: "delete", Client: v, Err: err}
		}
		return dr, &client.Error{Op: "delete", Client: v, Err: errors.New(s.Message())}
	}

	log.Trace(fmt.Sprintf("[volume] delete response: %s", resp.String()))

	for _, r := range resp.GetResults() {
		n := Needle{
			FileID: r.FileId,
			Size:   r.Size,
			Volume: v.ID().Host(),
		}

		if s := r.GetError(); s != "" {
			errNotFound := &client.Error{Op: "delete", Client: v, Err: ErrNotFound}
			if s != "not found" {
				n.err = &client.Error{Op: "delete", Client: v, Err: errors.New(s)}
				dr.Needles = append(dr.Needles, n)
				return dr, n.Err()
			}
			n.err = errNotFound
		}
		dr.Needles = append(dr.Needles, n)
	}
	return dr, nil
}
