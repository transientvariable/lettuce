package lettuce

import (
	"context"

	"github.com/transientvariable/lettuce/cluster"
	"github.com/transientvariable/lettuce/cluster/filer"

	gohttp "net/http"
)

// WithContext sets the context.Context used by the File.
func WithContext(ctx context.Context) func(*File) {
	return func(f *File) {
		f.ctxParent = ctx
	}
}

// WithEntry sets the filer.Entry metadata for the File.
func WithEntry(e *filer.Entry) func(*File) {
	return func(f *File) {
		f.entry = e
	}
}

// WithHTTPClient sets the http.Client used for read/write operations for a File.
func WithHTTPClient(c *gohttp.Client) func(*File) {
	return func(f *File) {
		f.client = c
	}
}

// WithCluster sets the cluster for communicating with SeaweedFS backend services.
func WithCluster(c *cluster.Cluster) func(*SeaweedFS) {
	return func(s *SeaweedFS) {
		s.cluster = c
	}
}

// WithGID sets the default group ID to use when writing data.
func WithGID(gid uint32) func(*SeaweedFS) {
	return func(s *SeaweedFS) {
		s.gid = int32(gid)
	}
}

// WithUID sets the default user ID to use when writing data.
func WithUID(uid uint32) func(*SeaweedFS) {
	return func(s *SeaweedFS) {
		s.uid = int32(uid)
	}
}
