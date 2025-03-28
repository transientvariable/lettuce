package filer

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	pathSeparator = string(os.PathSeparator)
)

var (
	entryFileFragmentPattern   = regexp.MustCompile(`.*\.(part)$`)
	entryTempResourcePattern   = regexp.MustCompile(`.*/?\.(uploads)/?`)
	entrySystemResourcePattern = regexp.MustCompile(`^/(topics)/?`)
)

// Path represents a file path within the context of the Filer.
type Path string

// Dir returns the directory for the Path.
func (p Path) Dir() string {
	return filepath.Dir(p.String())
}

// IsFileFragment returns whether the Path represents a fragment which is a piece, shard, etc. of a file.
func (p Path) IsFileFragment() bool {
	return entryFileFragmentPattern.MatchString(p.Name())
}

// IsTempResource returns whether the Path represents a temporary resource (e.g. **/.uploads).
func (p Path) IsTempResource() bool {
	return entryTempResourcePattern.MatchString(p.String())
}

// IsRoot returns whether the Path represents a root path.
func (p Path) IsRoot() bool {
	return p.Root() == p.String()
}

// IsSystemResource returns whether the Path represents a system resource (e.g. /topics/**).
func (p Path) IsSystemResource() bool {
	return entrySystemResourcePattern.MatchString(p.String())
}

// Name returns the file or directory name for the Path.
func (p Path) Name() string {
	return filepath.Base(p.String())
}

// Root returns the root part of the Path.
func (p Path) Root() string {
	return filepath.Join(pathSeparator, p.Split()[0])
}

// Split slices value returned by Path.String() separated by os.PathSeparator and returns the slice of the substrings.
func (p Path) Split() []string {
	return split(p.String())
}

// String returns a string representing the Path.
func (p Path) String() string {
	return string(p)
}

func split(p string) []string {
	var e []string
	for _, s := range strings.Split(p, pathSeparator) {
		if s != "" {
			e = append(e, s)
		}
	}
	return e
}
