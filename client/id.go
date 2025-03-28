package client

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/transientvariable/support-go"
)

const (
	grpcPortMask int = 10000
)

// ID is used for uniquely identifying an API client.
type ID struct {
	addr         url.URL
	grpcPortMask int
	name         string
}

// NewID creates a new ID with the provided address string.
func NewID(addr string, options ...func(*ID)) (ID, error) {
	if addr = strings.TrimSpace(addr); addr == "" {
		return ID{}, &Error{Err: errors.New("address is required for ID")}
	}

	if !support.URISchemePattern.MatchString(addr) {
		addr = HTTPURIScheme + "://" + addr
	}

	a, err := url.Parse(addr)
	if err != nil {
		return ID{}, &Error{Err: err}
	}

	id := ID{addr: EncodeAddr(*a)}
	for _, opt := range options {
		opt(&id)
	}

	if p, _ := strconv.Atoi(id.addr.Port()); p > 0 {
		if id.grpcPortMask > 0 {
			id.grpcPortMask += p
		} else {
			id.grpcPortMask = grpcPortMask + p
		}

		if _, err = net.ResolveTCPAddr("", fmt.Sprintf("%s:%d", id.addr.Hostname(), p)); err != nil {
			return id, fmt.Errorf("client: %w", err)
		}
	}
	return id, nil
}

// Addr returns the url.URL representing address associated with the ID.
func (i ID) Addr() url.URL {
	return i.addr
}

// GRPCAddr returns the gRPC address associated with the ID.
func (i ID) GRPCAddr() string {
	return net.JoinHostPort(i.Hostname(), strconv.Itoa(i.grpcPortMask))
}

// Host returns the `hostname:port` for the ID.
func (i ID) Host() string {
	return net.JoinHostPort(i.Hostname(), i.Port())
}

// Hostname returns the hostname attribute for the ID.
func (i ID) Hostname() string {
	return i.addr.Hostname()
}

// Name returns the name attribute for the ID.
func (i ID) Name() string {
	return i.name
}

// Port returns the port attribute for the ID.
func (i ID) Port() string {
	return i.addr.Port()
}

// String returns a string representation of the ID.
func (i ID) String() string {
	return string(support.ToJSONFormatted(map[string]any{
		"id": map[string]any{
			"hostname": i.Hostname(),
			"name":     i.Name(),
			"port":     i.Port(),
		},
	}))
}

// WithGRPCPortMask sets the port mask to use when creating the net.Addr representing the gRPC address for an ID.
//
// Default: 10000.
func WithGRPCPortMask(mask uint) func(*ID) {
	return func(id *ID) {
		id.grpcPortMask = int(mask)
	}
}

// WithName sets the name for the ID.
func WithName(name string) func(*ID) {
	return func(id *ID) {
		id.name = name
	}
}
