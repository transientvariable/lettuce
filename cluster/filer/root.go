package filer

import (
	"github.com/transientvariable/anchor"

	gofs "io/fs"
)

// Root represents the root of the Filer storage backend.
type Root struct {
	entry *Entry
	path  Path
}

// Entry returns the Entry representing the Root.
//
// Calling Entry.Path() will return the runtime path for the Root (e.g. `/`).
func (r Root) Entry() *Entry {
	return r.entry
}

// Path returns the Path for the Root.
//
// Calling Path.String() on the returned value will produce the path Root is mounted to for a Filer. This value will be
// the same as the Config.DirBuckets attribute for a Filer.
func (r Root) Path() Path {
	return r.path
}

// String returns a string representation of Root.
func (r Root) String() string {
	s := map[string]any{
		"mount": r.Path().String(),
		"path":  r.Entry().Path(),
	}
	if r.entry.PB() != nil && r.entry.PB().GetAttributes() != nil {
		s["mode"] = gofs.FileMode(r.entry.PB().GetAttributes().GetFileMode()).String()
	}
	return string(anchor.ToJSONFormatted(map[string]any{"root": s}))
}
