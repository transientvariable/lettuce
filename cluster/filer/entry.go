package filer

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/transientvariable/lettuce/chunk"
	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/support-go"

	json "github.com/json-iterator/go"
)

// Collection is container for properties that represent a `bucket` within the context of a SeaweedFS filer.
type Collection struct {
	GID  uint32 `json:"gid"`
	Name string `json:"name"`
	UID  uint32 `json:"uid"`
}

// String returns a string representation of the Collection.
func (c Collection) String() string {
	return string(support.ToJSONFormatted(c))
}

// Entry is a container for file and directory metadata managed by a SeaweedFS filer.
//
// The methods Entry.FileInfo() and Entry.DirEntry() can be used for retrieving fs.FileInfo and fs.DirEntry,
// respectively.
type Entry struct {
	chunks     *chunk.Chunks
	collection *Collection
	mutex      sync.Mutex
	path       Path
	pbEntry    *filer_pb.Entry
	size       int64
}

func newEntry(path Path, pbEntry *filer_pb.Entry) (*Entry, error) {
	e := &Entry{
		path:    path,
		pbEntry: pbEntry,
	}

	cks, err := chunk.NewChunks(path.String(), chunk.WithEntry(pbEntry), chunk.WithOnAdd(e.update))
	if err != nil {
		return nil, err
	}
	e.chunks = cks
	return e, nil
}

// Chunks returns the Chunks representing the content metadata for the Entry.
func (e *Entry) Chunks() *chunk.Chunks {
	return e.chunks
}

// Collection returns the Entry Collection.
func (e *Entry) Collection() Collection {
	if e.collection == nil {
		return Collection{}
	}
	return *e.collection
}

// FileIDs returns the list containing the file ID for each chunk.
func (e *Entry) FileIDs() ([]string, error) {
	cks, err := e.Chunks().List()
	if err != nil {
		return nil, err
	}

	fids := make([]string, e.Chunks().Len())
	iter := cks.Iterate()
	var i int
	for iter.HasNext() {
		c, err := iter.Next()
		if err != nil {
			return nil, err
		}
		fids[i] = c.FileID()
		i++
	}
	return fids, nil
}

// GID returns the group ID for the Entry.
func (e *Entry) GID() int32 {
	if e.pbEntry.GetAttributes() != nil {
		return int32(e.pbEntry.GetAttributes().GetGid())
	}
	return client.GID
}

// IsDir returns whether the Entry represents is a directory.
func (e *Entry) IsDir() bool {
	return e.pbEntry.GetIsDirectory()
}

// ModTime returns the modification time for the Entry.
func (e *Entry) ModTime() time.Time {
	if e.pbEntry.GetAttributes() != nil {
		return time.Unix(e.pbEntry.GetAttributes().GetMtime(), 0)
	}
	return time.Time{}
}

// Name returns the name for the Entry.
func (e *Entry) Name() string {
	return e.Path().Name()
}

// Path returns the storage Path for the Entry.
func (e *Entry) Path() Path {
	return e.path
}

// PB returns the protobuf entry if present.
func (e *Entry) PB() *filer_pb.Entry {
	return e.pbEntry
}

// Size returns the size of the Entry.
func (e *Entry) Size() int64 {
	if !e.PB().GetIsDirectory() && e.PB().GetAttributes() != nil {
		return int64(e.PB().GetAttributes().GetFileSize())
	}
	return 0
}

// ToMap returns a map of the Entry properties.
func (e *Entry) ToMap() (map[string]any, error) {
	var m map[string]any
	if err := json.NewDecoder(strings.NewReader(e.String())).Decode(&m); err != nil {
		return m, err
	}
	return m, nil
}

// Truncate removes all chunks and sets the size to 0 if the Entry is not a directory.
//
// The same operations will be performed on the protobuf entry if present.
func (e *Entry) Truncate() {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if !e.pbEntry.GetIsDirectory() && e.pbEntry.GetAttributes() != nil {
		e.pbEntry.GetAttributes().FileSize = 0
		e.pbEntry.Chunks = e.pbEntry.Chunks[:0]
		e.chunks.Clear()
	}
}

// UID returns the group ID for the Entry.
func (e *Entry) UID() int32 {
	if e.pbEntry.GetAttributes() != nil {
		return int32(e.pbEntry.GetAttributes().GetUid())
	}
	return client.UID
}

// String returns a string representation of the Entry.
func (e *Entry) String() string {
	s := make(map[string]any)
	if e.chunks != nil {
		s["chunks"] = e.chunks.ToMap()
	}

	if e.collection != nil {
		s["collection"] = e.collection
	}
	s["name"] = e.Name()
	s["path"] = e.Path().String()
	s["is_dir"] = e.IsDir()
	s["size"] = e.Size()
	return string(support.ToJSONFormatted(s))
}

func (e *Entry) update(chunks *chunk.Chunks) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if chunks == nil {
		return errors.New("filer_entry: failed to update entry, chunks not provided")
	}

	entries, err := chunks.PB()
	if err != nil {
		return err
	}

	pb := e.PB()
	if !pb.GetIsDirectory() && pb.GetAttributes() != nil && len(entries) > 0 {
		attrs := e.pbEntry.GetAttributes()
		attrs.FileSize = uint64(chunks.Size())
		pb.Chunks = entries
	}
	return nil
}
