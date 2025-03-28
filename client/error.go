package client

import (
	"strings"
)

// Enumeration of errors that may be returned by a SeaweedFS API client.
const (
	ErrClosed  = clientError("client already closed")
	ErrInvalid = clientError("invalid argument")
)

// clientError defines the type for errors that may be returned by a SeaweedFS API client.
type clientError string

// Error returns the cause of a SeaweedFS API client error.
func (e clientError) Error() string {
	return string(e)
}

// Error records an error for a client and the operation that caused it.
type Error struct {
	Err    error  `json:"-"`
	Client Client `json:"client,omitempty"`
	Op     string `json:"operation,omitempty"`
}

func (e *Error) Error() string {
	var msg strings.Builder

	var id ID
	if e.Client != nil {
		id = e.Client.ID()
	}

	if id.Name() != "" {
		msg.WriteString(id.Name())
	} else {
		msg.WriteString("client")
	}
	msg.WriteString(": ")

	if id.Host() != "" {
		msg.WriteString("host=" + id.Host() + " ")
	}

	if e.Op != "" {
		msg.WriteString("operation=" + e.Op + " ")
	}

	msg.WriteString("desc=")
	if e.Err != nil {
		msg.WriteString(e.Err.Error())
	} else {
		msg.WriteString("unknown")
	}
	return msg.String()
}

func (e *Error) Unwrap() error {
	return e.Err
}
