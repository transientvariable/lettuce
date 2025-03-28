package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/transientvariable/config"
	"github.com/transientvariable/configpath"
	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/cluster/filer"
	"github.com/transientvariable/lettuce/cluster/master"
	"github.com/transientvariable/lettuce/cluster/volume"
	"github.com/transientvariable/log"
	"github.com/transientvariable/support-go"

	gofs "io/fs"
)

// Cluster aggregates all SeaweedFS services into single Cluster.
type Cluster struct {
	closed  bool
	filer   *filer.Filer
	master  *master.Master
	mutex   sync.Mutex
	volumes map[client.ID]*volume.Volume
}

// New creates a SeaweedFS Cluster.
func New(options ...func(*Cluster)) (*Cluster, error) {
	c := &Cluster{}
	for _, opt := range options {
		opt(c)
	}

	if c.master == nil {
		addr := config.ValueMustResolve(configpath.SeaweedFSClusterMasterAddr)

		log.Warn("[cluster] master client not provided, creating default...")

		m, err := master.New(addr)
		if err != nil {
			return nil, fmt.Errorf("cluster: %w", err)
		}
		c.master = m
	}

	if c.filer == nil {
		addr := config.ValueMustResolve(configpath.SeaweedFSClusterFilerAddr)

		log.Warn("[cluster] filer client not provided, creating default...")

		f, err := filer.New(addr)
		if err != nil {
			return nil, fmt.Errorf("cluster: %w", err)
		}
		c.filer = f
	}

	vols, err := c.prepareVolumes()
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}
	c.volumes = vols

	log.Debug(fmt.Sprintf("[cluster] config: %s\n", c))
	return c, nil
}

// Close releases any resources used by the SeaweedFS Cluster.
func (c *Cluster) Close() error {
	log.Debug("[cluster] close")

	if c == nil {
		return gofs.ErrInvalid
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.closed {
		c.closed = true
		if c.filer != nil {
			return c.filer.Close()
		}

		if c.master != nil {
			return c.master.Close()
		}
		return nil
	}
	return fmt.Errorf("cluster: %w", gofs.ErrClosed)
}

// Filer returns the filer.Filer API client used by the Cluster.
func (c *Cluster) Filer() *filer.Filer {
	return c.filer
}

// Master returns the master.Master API client used by the Cluster.
func (c *Cluster) Master() *master.Master {
	return c.master
}

// Volume returns the volume.Volume API client matching the provided host. The host can be either the `hostname`
// (e.g. 0.0.0.0), or the `hostname:port` (e.g. 0.0.0.0:8080).
//
// If the host string is empty or if no API client is found, and error will be returned.
func (c *Cluster) Volume(host string) (*volume.Volume, error) {
	if host = strings.TrimSpace(host); host == "" {
		return nil, errors.New("cluster: volume host is required")
	}

	for k, v := range c.volumes {
		if k.Host() == host || k.Hostname() == host {
			return v, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("cluster: volume not found for host: %s", host))
}

// Volumes returns a list API clients for all volume servers known to the Cluster.
func (c *Cluster) Volumes() []*volume.Volume {
	var vols []*volume.Volume
	for _, v := range c.volumes {
		vols = append(vols, v)
	}
	return vols
}

// String returns a string representation of the Cluster.
func (c *Cluster) String() string {
	s := make(map[string]any)
	if c.Filer() != nil {
		s["filer"] = clientConfig(c.Filer())
	}

	if c.Filer() != nil {
		s["master"] = clientConfig(c.Master())
	}

	vols := c.Volumes()
	if len(vols) > 0 {
		var volConfigs []map[string]any
		for _, v := range vols {
			volConfigs = append(volConfigs, clientConfig(v))
		}
		s["volumes"] = volConfigs
	}
	return string(support.ToJSONFormatted(map[string]any{"cluster": s}))
}

func (c *Cluster) prepareVolumes() (map[client.ID]*volume.Volume, error) {
	addrs, err := c.Master().VolumeAddresses(context.Background())
	if err != nil {
		return nil, err
	}

	vols := make(map[client.ID]*volume.Volume)
	for _, addr := range addrs {
		log.Debug("[cluster] preparing volume client", log.String("address", addr.Host))

		v, err := volume.New(addr.Host)
		if err != nil {
			return vols, err
		}
		vols[v.ID()] = v
	}
	return vols, err
}

func clientConfig(client client.Client) map[string]any {
	if client != nil {
		c, err := client.Config()
		if err != nil {
			return map[string]any{"error": err.Error()}
		}
		return c
	}
	return nil
}

// WithFiler ...
func WithFiler(filer *filer.Filer) func(*Cluster) {
	return func(c *Cluster) {
		c.filer = filer
	}
}

// WithMaster ...
func WithMaster(master *master.Master) func(*Cluster) {
	return func(c *Cluster) {
		c.master = master
	}
}
