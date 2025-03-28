package watch

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"hash/adler32"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/transientvariable/config"
	"github.com/transientvariable/configpath"
	"github.com/transientvariable/event"
	"github.com/transientvariable/lettuce"
	"github.com/transientvariable/lettuce/cluster/filer"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log"
	"github.com/transientvariable/repository/ipfs"
	"github.com/transientvariable/repository/opensearch"
	"github.com/transientvariable/schema"
	"github.com/transientvariable/service/catalog"
	"github.com/transientvariable/support"
	"github.com/transientvariable/typeconv"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/go-units"

	weedsprt "github.com/transientvariable/lettuce/support"
	storageschema "github.com/transientvariable/schema/storage"
	goio "io"
	gofs "io/fs"
)

const (
	readMaxRetries = 5
)

// watcher defines the configuration for a service that watches for metadata events produced by SeaweedFS.
type watcher struct {
	closed      bool
	ctlg        catalog.Catalog
	docs        *opensearch.Repository
	ipfsEnabled bool
	ipfsRepo    *ipfs.Repository
	mutex       sync.Mutex
	options     *Option
	root        string
	weed        *seaweedfs.SeaweedFS
}

// NewWatcher creates a service that watches for metadata events using the provided seaweedfs.SeaweedFS backend and
// options.
func NewWatcher(weed *seaweedfs.SeaweedFS, options ...func(*Option)) (event.Watcher, error) {
	if weed == nil {
		return nil, errors.New("seaweedfs_watcher: seaweedfs file system is required")
	}

	opts := &Option{pathPrefix: weed.Cluster().Filer().PathSeparator()}
	for _, opt := range options {
		opt(opts)
	}

	if opts.pathPrefix == "" {
		return nil, errors.New("seaweedfs_watcher: path for watch events is required")
	}

	ctlg, err := catalog.New()
	if err != nil {
		return nil, err
	}

	r, err := weed.Root()
	if err != nil {
		return nil, err
	}

	ipfsEnabled, _ := config.Bool(configpath.RepositoryIPFSEnable)
	w := &watcher{
		ctlg:        ctlg,
		docs:        opensearch.New(),
		ipfsEnabled: ipfsEnabled,
		options:     opts,
		root:        r,
		weed:        weed,
	}

	if w.ipfsEnabled {
		w.ipfsRepo = ipfs.New()
	}
	return w, nil
}

// Run ...
func (w *watcher) Run(ctx context.Context) (<-chan *storageschema.Event, error) {
	log.Info("[seaweedfs:watcher] begin listening for metadata events")

	mc, err := newMetadataClient(ctx, w.weed.Cluster().Filer(), w.options)
	if err != nil {
		return nil, err
	}

	eventStream := make(chan *storageschema.Event)
	go func() {
		defer func(mc *metadataClient) {
			if err := mc.Close(); err != nil {
				log.Error("[seaweedfs:watcher]", log.Err(err))
			}
		}(mc)
		defer close(eventStream)

		for {
			resp, err := mc.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Warn("[seaweedfs:watcher] terminating event stream")
				} else {
					log.Error("[seaweedfs:watcher] could not receive event on stream", log.Err(err))
				}
				return
			}

			log.Trace(fmt.Sprintf("[seaweedfs:watcher] received event:\n%s", support.ToJSONFormatted(resp)))

			if dir := resp.GetDirectory(); dir == w.options.pathPrefix || dir == w.root {
				log.Trace(fmt.Sprintf("[seaweedfs:watcher] skipping event for directory: %s", dir))
				continue
			}

			evts, err := w.prepareEvents(ctx, resp)
			if err != nil {
				log.Error("[seaweedfs:watcher] could not prepare events", log.Err(err))
				continue
			}

			for _, e := range evts {
				select {
				case eventStream <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return eventStream, nil
}

// Close releases any resources used by the watcher.
func (w *watcher) Close() error {
	if w == nil {
		return gofs.ErrInvalid
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.closed {
		w.closed = true
		if w.weed != nil {
			if err := w.weed.Close(); err != nil && !errors.Is(err, gofs.ErrClosed) {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("seaweedfs_watcher: %w", gofs.ErrClosed)
}

// String returns string representation of the watcher.
func (w *watcher) String() string {
	s := make(map[string]any)
	s["filer_root"] = w.root

	if w.options != nil {
		om := w.options.options()
		if len(om) > 0 {
			s["options"] = w.options.options()
		}
	}
	return string(support.ToJSONFormatted(s))
}

func (w *watcher) prepareEvents(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse) ([]*storageschema.Event, error) {
	m := resp.GetEventNotification()

	oldEntry, err := w.newEntry(resp.GetDirectory(), m.GetOldEntry())
	if err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return nil, err
		}
	}

	newEntry, err := w.newEntry(m.GetNewParentPath(), m.GetNewEntry())
	if err != nil {
		if !errors.Is(err, gofs.ErrNotExist) {
			return nil, err
		}
	}

	var events []*storageschema.Event
	if oldEntry != nil && !oldEntry.Path().IsTempResource() && !oldEntry.Path().IsSystemResource() {
		log.Trace(fmt.Sprintf("[seaweedfs:watcher] processing old chunk:\n%s", oldEntry))

		ctlgEntry, err := w.ctlg.Find(oldEntry.Path().String())
		if err != nil {
			return nil, err
		}

		if _, ok := w.options.nsExcludes[ctlgEntry.Namespace]; !ok {
			if newEntry != nil &&
				(newEntry.IsDir() ||
					oldEntry.Path().String() != newEntry.Path().String() ||
					oldEntry.Name() != newEntry.Name()) {
				e, err := w.prepareEvent(ctx, ctlgEntry.Namespace, oldEntry, schema.EventTypeDeletion)
				if err != nil {
					return nil, err
				}
				events = append(events, e)
			}
		}
	}

	if newEntry != nil && !newEntry.Path().IsTempResource() && !newEntry.Path().IsSystemResource() {
		log.Trace(fmt.Sprintf("[seaweedfs:watcher] new chunk:\n%s", newEntry))

		eventType := schema.EventTypeCreation
		if oldEntry != nil && !oldEntry.IsDir() && oldEntry.Path() == newEntry.Path() {
			eventType = schema.EventTypeChange
		}

		ctlgEntry, err := w.ctlg.Find(newEntry.Path().String())
		if err != nil {
			return nil, err
		}

		if _, ok := w.options.nsExcludes[ctlgEntry.Namespace]; !ok {
			e, err := w.prepareEvent(ctx, ctlgEntry.Namespace, newEntry, eventType)
			if err != nil {
				return nil, err
			}
			events = append(events, e)
		}
	}
	return events, nil
}

func (w *watcher) prepareEvent(ctx context.Context, namespace string, entry *filer.Entry, eventType string) (*storageschema.Event, error) {
	fsEntry, err := seaweedfs.FSEntry(w.weed, entry)
	if err != nil {
		return nil, err
	}

	m, err := typeconv.FileMetadata(w.weed, fsEntry)
	if err != nil {
		return nil, err
	}

	log.Trace(fmt.Sprintf("[seaweedfs:watcher] preparing storage event for chunk: \n%s", support.ToJSONFormatted(m)))

	// Entries representing metadata fragments, e.g. those with the `.part` extension in the metadata name do not need
	// further processing to generate a storage event.
	if entry.Path().IsFileFragment() {
		return storageschema.NewStorageEvent(eventType, namespace+storageschema.NamespaceFragmentUpload, m)
	}

	start := time.Now()

	if !entry.IsDir() {
		p := strings.TrimPrefix(fsEntry.Path(), w.weed.PathSeparator())

		// Add chunk to IPFS
		if w.ipfsEnabled && w.ctlg.IPFSEnabled(namespace) {
			cid, err := w.ipfsRepo.Add(ctx, p)
			if err != nil {
				return nil, err
			}
			m.CID = cid.String()
		}

		// Generate hash digests(s)
		h, s, err := w.prepareHash(p)
		if err != nil {
			return nil, err
		}
		m.Hash = h
		m.Size = s
	}

	log.Debug("[seaweedfs:watcher] generated metadata",
		log.String("cid", m.CID),
		log.String("metadata", m.Path),
		log.Any("hash", m.Hash),
		log.Int64("size", m.Size),
		log.String("took", units.HumanDuration(time.Since(start))))
	return storageschema.NewStorageEvent(eventType, namespace, m)
}

func (w *watcher) prepareHash(path string) (*schema.Hash, int64, error) {
	log.Trace("[seaweedfs:watcher] preparing hash(es)", log.String("path", path))

	sha256Sum := sha256.New()
	hashes := map[string]hash.Hash{"sha256": sha256Sum}
	writers := []io.Writer{sha256Sum}

	for _, a := range w.options.hashAglos {
		switch a {
		case "adler32":
			h := adler32.New()
			hashes["adler32"] = h
			writers = append(writers, h)
			break
		case "md5":
			h := md5.New()
			hashes["md5"] = h
			writers = append(writers, h)
			break
		case "sha256":
			// no-op, SHA256 is generated in all cases
			break
		default:
			return nil, 0, errors.New(fmt.Sprintf("seaweedfs_watcher: unsupported hash algorithm: %s", a))
		}
	}
	writer := goio.MultiWriter(writers...)

	err := backoff.Retry(func() error {
		if _, err := w.weed.Stat(path); err != nil {
			return err
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), readMaxRetries))

	wf, err := w.weed.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func(f gofs.File) {
		if err := f.Close(); err != nil {
			log.Error("[seaweedfs:watcher]", log.Err(err))
		}
	}(wf)

	buf := weedsprt.AcquireBuffer()
	defer weedsprt.ReleaseBuffer(buf)
	s, err := goio.CopyBuffer(writer, wf, buf)
	if err != nil {
		return nil, 0, err
	}

	h := &schema.Hash{}
	for n, a := range hashes {
		switch n {
		case "adler32":
			h.Adler32 = fmt.Sprintf("%x", a.Sum(nil))
			break
		case "md5":
			h.Md5 = fmt.Sprintf("%x", a.Sum(nil))
			break
		case "sha256":
			h.Sha256 = fmt.Sprintf("%x", a.Sum(nil))
			break
		default:
			// no-op
		}
	}
	return h, s, nil
}

func (w *watcher) newEntry(dir string, pbEntry *filer_pb.Entry) (*filer.Entry, error) {
	if dir = strings.TrimSpace(dir); strings.HasPrefix(dir, w.options.pathPrefix) {
		dir = dir[1:]
	}

	if dir != "" && pbEntry != nil {
		return w.weed.Cluster().Filer().NewEntry(dir, pbEntry)
	}
	return nil, gofs.ErrNotExist
}
