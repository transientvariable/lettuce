package volume

// Enumeration of errors that may be returned by a SeaweedFS volume API client.
const (
	ErrNotFound = volumeError("not found")
)

// volumeError defines the type for errors that may be returned by a SeaweedFS volume server.
type volumeError string

// Error returns the cause of a SeaweedFS volume server error.
func (e volumeError) Error() string {
	return string(e)
}
