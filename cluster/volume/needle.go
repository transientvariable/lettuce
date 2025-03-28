package volume

import (
	"fmt"

	"github.com/transientvariable/support-go"
)

// Needle represents volume data for a single file ID.
//
// Needle.Err() can be used to assert whether the Needle has an associated error.
type Needle struct {
	FileID string `json:"file_id,omitempty"`
	Size   uint32 `json:"size,omitempty"`
	Volume string `json:"volume,omitempty"`
	err    error
}

// Err returns the error for the Needle.
func (n Needle) Err() error {
	if n.err != nil {
		if n.FileID != "" {
			return fmt.Errorf("needle: file ID %s %w", n.FileID, n.err)
		}
		return fmt.Errorf("needle: %w", n.err)
	}
	return nil
}

// String returns a string representation of the Needle.
func (n Needle) String() string {
	if err := n.Err(); err != nil {
		return err.Error()
	}
	return string(support.ToJSONFormatted(n))
}
