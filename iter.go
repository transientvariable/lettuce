package lettuce

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/transientvariable/fs-go"
	"github.com/transientvariable/lettuce/cluster/filer"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log-go"
)

type dirEntry struct {
	entry *filer.Entry
	err   error
}

type dirIterator struct {
	ctx     context.Context
	dir     *filer.Entry
	entries <-chan dirEntry
	filer   *filer.Filer
	hasNext atomic.Bool
	mutex   sync.Mutex
	name    string
	next    dirEntry
	weed    *SeaweedFS
}

func newDirIterator(ctx context.Context, weed *SeaweedFS, entry *filer.Entry) (fs.DirIterator, error) {
	if weed == nil {
		return nil, errors.New("dir_iterator: file system is required")
	}

	if !entry.IsDir() {
		return nil, fs.ErrNotDir
	}

	log.Trace("[dir_iterator] listing entries",
		log.Bool("is_dir", entry.IsDir()),
		log.String("name", entry.Name()),
		log.String("path", entry.Path().String()))

	f := weed.cluster.Filer()
	c, err := f.PB().ListEntries(ctx, &filer_pb.ListEntriesRequest{
		Directory: entry.Path().String(),
	})
	if err != nil {
		return nil, err
	}

	iter := &dirIterator{
		ctx:     ctx,
		dir:     entry,
		entries: read(ctx, f, entry, c),
		filer:   f,
		name:    entry.Name(),
		weed:    weed,
	}
	iter.hasNext.Swap(true)
	return iter, nil
}

// HasNext returns whether the directory has remaining list.
func (i *dirIterator) HasNext() bool {
	return i.hasNext.Load()
}

// Next returns the next directory entry. Dot list "." are skipped.
//
// The error io.EOF is returned if there are no remaining list left to iterate.
func (i *dirIterator) Next() (*fs.Entry, error) {
	if !i.HasNext() {
		return nil, io.EOF
	}

	de := <-i.entries
	if de.err != nil {
		i.hasNext.Swap(false)
		return nil, de.err
	}

	e, err := FSEntry(i.weed, de.entry)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// NextN returns a slice containing the next n directory list. Dot list "." are skipped.
//
// The error io.EOF is returned if there are no remaining list left to iterate.
func (i *dirIterator) NextN(n int) ([]*fs.Entry, error) {
	var entries []*fs.Entry
	if n > 0 {
		for j := 0; j < n; j++ {
			e, err := i.Next()
			if err != nil {
				return entries, err
			}
			entries = append(entries, e)
		}
		return entries, nil
	}

	for i.HasNext() {
		e, err := i.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return entries, nil
			}
			return entries, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func read(ctx context.Context, filer *filer.Filer, dir *filer.Entry, c filer_pb.SeaweedFiler_ListEntriesClient) <-chan dirEntry {
	entries := make(chan dirEntry)
	go func() {
		defer close(entries)

		for {
			dirEntry := dirEntry{}
			resp, err := c.Recv()
			if err != nil {
				dirEntry.err = err
				entries <- dirEntry
				break
			}

			n, err := filer.NewEntry(dir.Name(), resp.GetEntry())
			if err != nil {
				dirEntry.err = err
				entries <- dirEntry
				break
			}
			dirEntry.entry = n

			select {
			case entries <- dirEntry:
			case <-ctx.Done():
				return
			}
		}
	}()
	return entries
}
