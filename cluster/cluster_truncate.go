package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/cluster/filer"
	"github.com/transientvariable/lettuce/cluster/volume"
	"github.com/transientvariable/log"
)

const (
	workers = 8
)

type volumeInfo struct {
	err     error
	fileID  string
	volumes []*volume.Volume
}

// Truncate removes data associated with the provided name, sets the size of the entry metadata to 0, and updates the
// modification time.
func (c *Cluster) Truncate(ctx context.Context, name string) (*filer.Entry, error) {
	entry, err := c.Filer().Stat(ctx, name)
	if err != nil {
		return nil, err
	}

	if entry.IsDir() {
		return entry, nil
	}

	log.Trace("[cluster] truncating entry",
		log.Int("chunks", entry.Chunks().Len()),
		log.String("name", entry.Name()))

	volumes, err := c.mapVolumes(ctx, entry)
	if err != nil {
		return entry, &client.Error{Op: "truncate", Err: err}
	}

	if len(volumes) > 0 {
		log.Trace("[cluster] found volumes containing entry data",
			log.String("name", entry.Name()),
			log.Int("volumes", len(volumes)))

		for id, fids := range volumes {
			v, err := c.Volume(id.Host())
			if err != nil {
				return entry, &client.Error{Op: "truncate", Err: err}
			}

			r, err := v.Delete(ctx, fids...)
			if err != nil {
				return entry, &client.Error{Op: "truncate", Err: err}
			}

			log.Trace(fmt.Sprintf("[cluster] deletion result: %s\n", r))
		}
	}

	entry.Truncate()
	if err = c.Filer().Update(ctx, entry); err != nil {
		return entry, &client.Error{Op: "truncate", Err: err}
	}

	log.Trace("[cluster] entry truncated",
		log.String("name", entry.Name()),
		log.Time("mod_time", entry.ModTime()))

	return entry, nil
}

func (c *Cluster) mapVolumes(ctx context.Context, entry *filer.Entry) (map[client.ID][]string, error) {
	fids, err := readFileIDs(ctx, entry)
	if err != nil {
		return nil, err
	}
	volumes := make(chan volumeInfo)

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			c.findVolumes(ctx, fids, volumes)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(volumes)
	}()

	vm := make(map[client.ID][]string)
	for vi := range volumes {
		if vi.err != nil {
			return vm, err
		}

		for _, v := range vi.volumes {
			fids, ok := vm[v.ID()]
			if !ok {
				vm[v.ID()] = []string{vi.fileID}
			}
			fids = append(fids, vi.fileID)
		}
	}
	return vm, nil
}

func (c *Cluster) findVolumes(ctx context.Context, fids <-chan string, volumes chan<- volumeInfo) {
	for fid := range fids {
		vi := volumeInfo{fileID: fid}
		addrs, err := c.Master().FindVolumes(ctx, "", fid)
		if err != nil {
			vi.err = err
		}

		if vi.err == nil && len(addrs) > 0 {
			for _, a := range addrs {
				v, err := c.Volume(a.Host)
				if err != nil {
					vi.err = err
					break
				}
				vi.volumes = append(vi.volumes, v)
			}
		}

		select {
		case volumes <- vi:
		case <-ctx.Done():
			return
		}
	}
}

func readFileIDs(ctx context.Context, entry *filer.Entry) (<-chan string, error) {
	fids, err := entry.FileIDs()
	if err != nil {
		return nil, err
	}

	log.Trace("[cluster] emitting file IDs", log.Int("size", len(fids)))

	out := make(chan string)
	go func() {
		defer close(out)
		for _, fid := range fids {
			select {
			case out <- fid:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
