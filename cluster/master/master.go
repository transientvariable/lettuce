package master

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/master_pb"
	"github.com/transientvariable/log-go"
	"github.com/transientvariable/support-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	json "github.com/json-iterator/go"
)

const (
	name = "master"
)

// Config represents Master server configuration attributes.
type Config struct {
	Leader                 string `json:"leader"`
	MetricsIntervalSeconds uint32 `json:"metrics_interval_seconds"`
	VolumePreallocate      bool   `json:"volume_preallocate"`
	VolumeSizeLimitMB      uint32 `json:"volume_size_limit_m_b"`
}

// Master represents a connection to a SeaweedFS master server.
type Master struct {
	client master_pb.SeaweedClient
	closed atomic.Bool
	config *Config
	conn   *grpc.ClientConn
	id     *client.ID
}

// New creates a new API client for performing operations on a SeaweedFS master server with the provided address.
func New(addr string) (*Master, error) {
	m, err := master(addr)
	if err != nil {
		return m, fmt.Errorf("master: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Ready(ctx, m)
	if err != nil {
		return m, fmt.Errorf("master: %w", err)
	}

	config := resp.(*master_pb.GetMasterConfigurationResponse)
	m.config = &Config{
		Leader:                 config.GetLeader(),
		MetricsIntervalSeconds: config.GetMetricsIntervalSeconds(),
		VolumePreallocate:      config.GetVolumePreallocate(),
		VolumeSizeLimitMB:      config.GetVolumeSizeLimitMB(),
	}

	a := m.id.Addr()
	log.Info("[master] initialized master API client", log.String("address", a.String()))

	return m, nil
}

// Addr returns the url.URL representing the HTTP/S address for the server that the Master API client is connected to.
func (m *Master) Addr() url.URL {
	return m.id.Addr()
}

// Close releases any resources used by the Master API client.
func (m *Master) Close() error {
	log.Debug("[master] close")

	if m == nil {
		return &client.Error{Op: "close", Err: client.ErrInvalid}
	}

	if !m.closed.Load() {
		m.closed.Swap(true)
		if m.conn != nil {
			if err := m.conn.Close(); err != nil {
				return &client.Error{Op: "close", Err: err}
			}
		}
		return nil
	}
	return &client.Error{Op: "close", Err: client.ErrClosed}
}

// Config returns the configuration attributes for the server the Master API client is connected to.
func (m *Master) Config() (map[string]any, error) {
	var c map[string]any
	if err := json.NewDecoder(strings.NewReader(m.String())).Decode(&c); err != nil {
		return c, err
	}
	return c, nil
}

// GRPCAddr returns the gRPC target for the server that the Master API client is connected to.
func (m *Master) GRPCAddr() string {
	return m.id.GRPCAddr()
}

// ID returns the client.ID for the Master API client.
func (m *Master) ID() client.ID {
	return *m.id
}

// Name returns the name for the Master API client.
func (m *Master) Name() string {
	return m.id.Name()
}

// PB returns the protobuf interface for the Master API client.
func (m *Master) PB() master_pb.SeaweedClient {
	return m.client
}

// Ready returns the master server configuration indicating that the API client has established a connection.
//
// An error will be returned if the configuration could not be retrieved.
func (m *Master) Ready(ctx context.Context) (any, error) {
	resp, err := m.PB().GetMasterConfiguration(ctx, &master_pb.GetMasterConfigurationRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return nil, fmt.Errorf("master: %w", errors.New(s.Message()))
		}
		return nil, fmt.Errorf("master: %w", err)
	}
	return resp, nil
}

// String returns a string representation of the Master API client.
func (m *Master) String() string {
	s := make(map[string]any)
	s["config"] = m.config
	s["id"] = map[string]any{
		"hostname": m.id.Hostname(),
		"name":     m.id.Name(),
		"port":     m.id.Port(),
	}
	s["grpc_address"] = m.GRPCAddr()
	return string(support.ToJSONFormatted(s))
}

func master(addr string) (*Master, error) {
	id, err := client.NewID(addr, client.WithName(name))
	if err != nil {
		return nil, err
	}

	m := &Master{id: &id}
	conn, err := client.NewClientConn(m)
	if err != nil {
		return nil, err
	}
	m.conn = conn

	m.client = master_pb.NewSeaweedClient(conn)
	return m, nil
}
