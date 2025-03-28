package volume

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/volume_server_pb"
	"github.com/transientvariable/log"
	"github.com/transientvariable/support"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	json "github.com/json-iterator/go"
)

const (
	name = "volume"
)

// Config represents Volume server configuration attributes.
type Config struct {
	DataCenter string                         `json:"data_center,omitempty"`
	DiskStatus []*volume_server_pb.DiskStatus `json:"disk_status,omitempty"`
	MemStatus  *volume_server_pb.MemStatus    `json:"memory_status,omitempty"`
	Rack       string                         `json:"rack,omitempty"`
	Version    string                         `json:"version,omitempty"`
}

// Volume represents a connection to a SeaweedFS volume server.
type Volume struct {
	client volume_server_pb.VolumeServerClient
	closed atomic.Bool
	conn   *grpc.ClientConn
	id     *client.ID
	config *Config
}

// New creates a new API client for performing operations on a SeaweedFS master volume with the provided address.
func New(addr string) (*Volume, error) {
	v, err := volume(addr)
	if err != nil {
		return v, &client.Error{Client: v, Err: err}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Ready(ctx, v)
	if err != nil {
		return v, &client.Error{Client: v, Err: err}
	}

	stat := resp.(*volume_server_pb.VolumeServerStatusResponse)
	v.config = &Config{
		DataCenter: stat.GetDataCenter(),
		DiskStatus: stat.GetDiskStatuses(),
		MemStatus:  stat.GetMemoryStatus(),
		Rack:       stat.GetRack(),
		Version:    stat.GetVersion(),
	}

	a := v.Addr()
	log.Info("[volume] initialized volume API client", log.String("address", a.String()))

	return v, nil
}

// Addr returns the url.URL representing the HTTP/S address for the server that the Volume API client is connected to.
func (v *Volume) Addr() url.URL {
	return v.id.Addr()
}

// Close releases any resources used by the current Volume server API client.
func (v *Volume) Close() error {
	if v == nil {
		return &client.Error{Op: "close", Err: client.ErrInvalid}
	}

	if !v.closed.Load() {
		v.closed.Swap(true)
		if v.conn != nil {
			if err := v.conn.Close(); err != nil {
				return &client.Error{Op: "close", Err: err}
			}
		}
		return nil
	}
	return &client.Error{Op: "close", Err: client.ErrClosed}
}

// Config returns the configuration attributes for the server the Volume API client is connected to.
func (v *Volume) Config() (map[string]any, error) {
	var c map[string]any
	if err := json.NewDecoder(strings.NewReader(v.String())).Decode(&c); err != nil {
		return c, &client.Error{Client: v, Err: err}
	}
	return c, nil
}

// GRPCAddr returns the gRPC target for the server that the Volume API client is connected to.
func (v *Volume) GRPCAddr() string {
	return v.id.GRPCAddr()
}

// ID returns the client.ID for the Volume server API client.
func (v *Volume) ID() client.ID {
	return *v.id
}

// Name returns the name for the Volume API client.
func (v *Volume) Name() string {
	return v.id.Name()
}

// PB returns the protobuf interface for the Volume server API client.
func (v *Volume) PB() volume_server_pb.VolumeServerClient {
	return v.client
}

// Ready returns the volume server status indicating that the API client has established a connection.
//
// An error will be returned if the status could not be retrieved.
func (v *Volume) Ready(ctx context.Context) (any, error) {
	resp, err := v.PB().VolumeServerStatus(ctx, &volume_server_pb.VolumeServerStatusRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return nil, &client.Error{Op: "ready", Client: v, Err: errors.New(s.Message())}
		}
		return nil, &client.Error{Op: "ready", Client: v, Err: err}
	}
	return resp, nil
}

// String returns a string representation of the Volume server API client.
func (v *Volume) String() string {
	s := make(map[string]any)
	s["config"] = v.config
	s["id"] = map[string]any{
		"hostname": v.id.Hostname(),
		"name":     v.id.Name(),
		"port":     v.id.Port(),
	}
	s["grpc_address"] = v.GRPCAddr()
	return string(support.ToJSONFormatted(s))
}

func volume(addr string) (*Volume, error) {
	id, err := client.NewID(addr, client.WithName(name))
	if err != nil {
		return nil, err
	}

	v := &Volume{id: &id}
	conn, err := client.NewClientConn(v)
	if err != nil {
		return nil, err
	}
	v.conn = conn
	v.client = volume_server_pb.NewVolumeServerClient(conn)
	return v, nil
}
