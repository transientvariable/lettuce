package filer

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/filer_pb"
)

// AssignVolume assigns a portion of file content (chunk) represented by the provided path to a volume server and
// returns the file ID and url.URL which can be used for writing data.
func (f *Filer) AssignVolume(ctx context.Context, path string) (string, url.URL, error) {
	if path = strings.TrimSpace(path); path == "" {
		return "", url.URL{}, &client.Error{Op: "assign", Client: f, Err: errors.New("path is required for assigning volume")}
	}

	resp, err := f.PB().AssignVolume(ctx, &filer_pb.AssignVolumeRequest{
		Count: 1,
		Path:  path,
	})
	if err != nil {
		return "", url.URL{}, &client.Error{Op: "assign", Client: f, Err: err}
	}
	return resp.GetFileId(), client.EncodeAddr(url.URL{
		Host:   resp.GetLocation().GetUrl(),
		Path:   resp.GetFileId(),
		Scheme: client.HTTPURIScheme,
	}), nil
}
