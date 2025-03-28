package filer

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log-go"
	"github.com/transientvariable/support-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	json "github.com/json-iterator/go"
	gofs "io/fs"
)

const (
	errNotFoundStr = "no entry is found"
	name           = "filer"
)

// Config represents Filer server configuration attributes.
type Config struct {
	DirBuckets         string   `json:"dir_buckets"`
	Masters            []string `json:"masters"`
	MaxMb              uint32   `json:"max_mb"`
	MetricsIntervalSec int32    `json:"metrics_interval_sec"`
	Signature          int32    `json:"signature"`
	Version            string   `json:"version"`
}

// String returns a string representation of the Config.
func (c Config) String() string {
	return string(support.ToJSONFormatted(c))
}

// Filer represents a connection to a SeaweedFS filer server.
type Filer struct {
	client    filer_pb.SeaweedFilerClient
	closed    atomic.Bool
	config    *Config
	conn      *grpc.ClientConn
	id        *client.ID
	root      *Root
	signature int32
}

// New creates a new API client for performing operations on a SeaweedFS filer server with the provided address.
func New(addr string) (*Filer, error) {
	f, err := filer(addr)
	if err != nil {
		return f, &client.Error{Client: f, Err: err}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Ready(ctx, f)
	if err != nil {
		return f, &client.Error{Client: f, Err: err}
	}

	config := resp.(*filer_pb.GetFilerConfigurationResponse)
	f.config = &Config{
		DirBuckets:         config.GetDirBuckets(),
		Masters:            config.GetMasters(),
		MaxMb:              config.GetMaxMb(),
		MetricsIntervalSec: config.GetMetricsIntervalSec(),
		Signature:          config.GetSignature(),
		Version:            config.GetVersion(),
	}

	if err := setRoot(ctx, f); err != nil {
		return f, &client.Error{Client: f, Err: err}
	}

	a := f.id.Addr()
	log.Info("[filer] initialized filer API client", log.String("address", a.String()))

	return f, nil
}

// Addr returns the url.URL representing the HTTP/S address for the server that the Filer API client is connected to.
func (f *Filer) Addr() url.URL {
	return f.id.Addr()
}

// Close releases any resources used by the Filer server API client.
func (f *Filer) Close() error {
	log.Debug("[filer] close")

	if f == nil {
		return &client.Error{Op: "close", Err: client.ErrInvalid}
	}

	if !f.closed.Load() {
		f.closed.Swap(true)
		if f.conn != nil {
			if err := f.conn.Close(); err != nil {
				return &client.Error{Op: "close", Err: err}
			}
		}
		return nil
	}
	return &client.Error{Op: "close", Err: client.ErrClosed}
}

// Config returns the configuration attributes for the server the Filer API client is connected to.
func (f *Filer) Config() (map[string]any, error) {
	var c map[string]any
	if err := json.NewDecoder(strings.NewReader(f.String())).Decode(&c); err != nil {
		return c, &client.Error{Client: f, Err: err}
	}
	return c, nil
}

// GRPCAddr returns the gRPC target for the server that the Filer API client is connected to.
func (f *Filer) GRPCAddr() string {
	return f.id.GRPCAddr()
}

// ID returns the client.ID for the Filer API client.
func (f *Filer) ID() client.ID {
	return *f.id
}

// Name returns the name for the Filer API client.
func (f *Filer) Name() string {
	return f.id.Name()
}

// NewEntry creates a new fs.Entry.
func (f *Filer) NewEntry(dir string, filerEntry *filer_pb.Entry) (*Entry, error) {
	if dir = strings.TrimSpace(dir); dir == "" {
		return nil, &client.Error{Op: "newEntry", Client: f, Err: errors.New("directory for entry is required")}
	}

	if filerEntry == nil {
		return nil, &client.Error{Op: "newEntry", Client: f, Err: errors.New("entry is required")}
	}

	if filerEntry.GetAttributes() == nil {
		return nil, &client.Error{Op: "newEntry", Client: f, Err: errors.New("entry attributes are missing")}
	}

	name := filerEntry.GetName()
	if dir != f.root.Entry().Name() {
		name = filepath.Join(dir, name)
	}

	path, err := f.path(name)
	if err != nil {
		return nil, &client.Error{Op: "newEntry", Client: f, Err: err}
	}

	log.Trace("[filer] creating entry ref",
		log.String("directory", dir),
		log.String("name", path.Name()),
		log.String("path", path.String()))

	e, err := newEntry(path, filerEntry)
	if err != nil {
		return nil, &client.Error{Op: "newEntry", Client: f, Err: fmt.Errorf("could not create new entry: %w", err)}
	}
	return e, nil
}

// PathSeparator returns the path separator used by the Filer server API client.
func (f *Filer) PathSeparator() string {
	return pathSeparator
}

// PB returns the protobuf interface for the Filer server API client.
func (f *Filer) PB() filer_pb.SeaweedFilerClient {
	return f.client
}

// Ready returns the filer server configuration indicating that the API client has established a connection.
//
// An error will be returned if the configuration could not be retrieved.
func (f *Filer) Ready(ctx context.Context) (any, error) {
	resp, err := f.PB().GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return nil, &client.Error{Op: "ready", Client: f, Err: errors.New(s.Message())}
		}
		return nil, &client.Error{Op: "ready", Client: f, Err: err}
	}
	return resp, nil
}

// Root returns the Root representing the root used by the Filer server API client.
func (f *Filer) Root() Root {
	return *f.root
}

// String returns a string representation of the Filer server API client.
func (f *Filer) String() string {
	s := make(map[string]any)
	s["config"] = f.config
	s["id"] = map[string]any{
		"hostname": f.id.Hostname(),
		"name":     f.id.Name(),
		"port":     f.id.Port(),
	}
	s["grpc_address"] = f.GRPCAddr()

	r := map[string]any{
		"mount": f.root.Path().String(),
		"name":  f.root.Entry().Name(),
	}
	if f.root.Entry().PB() != nil && f.root.Entry().PB().GetAttributes() != nil {
		r["mode"] = gofs.FileMode(f.root.Entry().PB().GetAttributes().GetFileMode()).String()
	}
	s["root"] = r
	return string(support.ToJSONFormatted(s))
}

func (f *Filer) path(name string) (Path, error) {
	name = filepath.Join(pathSeparator, filepath.Clean(strings.TrimSpace(name)))
	if r := f.root.Path().Root(); !strings.HasPrefix(name, r) {
		name = filepath.Join(r, name)
	}
	return Path(name), nil
}

func filer(addr string) (*Filer, error) {
	id, err := client.NewID(addr, client.WithName(name))
	if err != nil {
		return nil, err
	}

	f := &Filer{id: &id}
	conn, err := client.NewClientConn(f)
	if err != nil {
		return nil, err
	}
	f.conn = conn
	f.client = filer_pb.NewSeaweedFilerClient(conn)
	return f, nil
}

func setRoot(ctx context.Context, filer *Filer) error {
	resp, err := filer.client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
		Directory: filer.config.DirBuckets,
	})
	if err != nil {
		if strings.Contains(err.Error(), errNotFoundStr) {
			return &client.Error{
				Client: filer,
				Op:     "setRoot",
				Err:    fmt.Errorf("%s: %w", filer.config.DirBuckets, gofs.ErrNotExist),
			}
		}
		return err
	}

	fe := resp.GetEntry()
	if fe == nil {
		return gofs.ErrNotExist
	}

	path := Path(filepath.Join(pathSeparator, filer.config.DirBuckets))
	e, err := newEntry(path, fe)
	if err != nil {
		return err
	}
	filer.root = &Root{entry: e, path: path}
	return nil
}
