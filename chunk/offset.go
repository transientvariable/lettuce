package chunk

import (
	"fmt"
)

// Offset represents a closed interval of bytes with a start and end position.
type Offset struct {
	Start int64
	End   int64
}

// After reports whether the start of the Offset occurs after the end offset of the provided one.
func (o Offset) After(off Offset) bool {
	return o.Start >= off.End
}

// Before reports whether the end of the Offset occurs before the start offset of the provided one.
func (o Offset) Before(off Offset) bool {
	return o.End <= off.Start
}

// Contains reports whether the Offset contains provided one.
func (o Offset) Contains(off int64) bool {
	return o.Start <= off && off <= o.End
}

// Equal reports whether the Offset contains the same start and end as the provided one.
func (o Offset) Equal(other Offset) bool {
	return o == other || o.IsEmpty() && other.IsEmpty()
}

// IsEmpty reports whether the Offset is empty.
func (o Offset) IsEmpty() bool {
	return o.Start > o.End
}

// Length returns the difference of end and start of the Offset.
func (o Offset) Length() int64 {
	return o.End - o.Start
}

// String returns a string representing the Offset using the format `Offset.Start:Offset.End`.
func (o Offset) String() string {
	return fmt.Sprintf("%d:%d", o.Start, o.End)
}
