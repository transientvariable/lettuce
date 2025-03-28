package watch

import (
	"strings"
	"time"

	"github.com/transientvariable/support"

	"google.golang.org/grpc"
)

// Option is a container for optional properties that can be used for customizing the behavior of metadata
// event subscriptions for the watcher.
type Option struct {
	clientID              int32
	clientName            string
	conn                  *grpc.ClientConn
	endpoint              string
	filerConn             *grpc.ClientConn
	filerEndpoint         string
	hashAglos             []string
	nsExcludes            map[string]bool
	pathPrefix            string
	pathPrefixes          []string
	signature             int32
	subscription          string
	timeOffsetBegin       time.Time
	timeOffsetEnd         time.Time
	writerChunkBufferSize int64
	writerConcurrency     int
}

// String returns a string representation of Option.
func (o *Option) String() string {
	return string(support.ToJSONFormatted(o.options()))
}

func (o *Option) options() map[string]any {
	m := make(map[string]any)
	m["client_id"] = o.clientID
	m["client_name"] = o.clientName

	if o.conn != nil {
		m["connection"] = o.conn.Target()
	}

	m["endpoint"] = o.endpoint
	m["hash_algorithms"] = o.hashAglos

	if o.nsExcludes != nil {
		m["namespace_excludes"] = o.nsExcludes
	}

	m["path_prefixes"] = o.pathPrefixes
	m["signature"] = o.signature
	m["subscription"] = o.subscription
	m["timeOffsetBegin"] = o.timeOffsetBegin.Format(time.RFC3339)
	m["timeOffsetEnd"] = o.timeOffsetEnd.Format(time.RFC3339)
	m["writerChunkBufferSize"] = o.writerChunkBufferSize
	m["writerConcurrency"] = o.writerConcurrency
	return m
}

// WithFilerConn ...
func WithFilerConn(conn *grpc.ClientConn) func(*Option) {
	return func(o *Option) {
		o.conn = conn
	}
}

// WithFilerEndpoint ...
func WithFilerEndpoint(endpoint string) func(*Option) {
	return func(o *Option) {
		o.endpoint = strings.TrimSpace(endpoint)
	}
}

// WithClientID sets the cluster ID used when subscribing to metadata events.
func WithClientID(id int32) func(*Option) {
	return func(o *Option) {
		o.clientID = id
	}
}

// WithClientName sets the cluster name used when subscribing to metadata events.
func WithClientName(name string) func(*Option) {
	return func(o *Option) {
		o.clientName = strings.TrimSpace(name)
	}
}

// WithHashAlgorithm sets the list of hash algorithms to use for generating metadata checksums.
func WithHashAlgorithm(algos ...string) func(*Option) {
	return func(o *Option) {
		for _, a := range algos {
			if a = strings.TrimSpace(a); a != "" {
				o.hashAglos = append(o.hashAglos, strings.ToLower(a))
			}
		}
	}
}

// WithNamespaceExcludes ...
func WithNamespaceExcludes(namespaces ...string) func(*Option) {
	return func(o *Option) {
		if len(namespaces) > 0 {
			if o.nsExcludes == nil {
				o.nsExcludes = make(map[string]bool)
			}

			for _, ns := range namespaces {
				if ns = strings.TrimSpace(ns); ns != "" {
					o.nsExcludes[ns] = true
				}
			}
		}
	}
}

// WithPathPrefixes sets additional path prefixes to watch when subscribing to metadata events.
func WithPathPrefixes(pathPrefixes ...string) func(*Option) {
	return func(o *Option) {
		if len(pathPrefixes) > 0 {
			o.pathPrefix = strings.TrimSpace(pathPrefixes[0])

			if len(pathPrefixes) > 1 {
				for _, p := range pathPrefixes[1:] {
					p = strings.TrimSpace(p)
					if p != "" {
						o.pathPrefixes = append(o.pathPrefixes, p)
					}
				}
			}
		}
	}
}

// WithSignature sets the signature used when subscribing to metadata event streams.
func WithSignature(signature int32) func(*Option) {
	return func(o *Option) {
		if signature > 0 {
			o.signature = signature
		}
	}
}

// WithSubscription sets the type of subscription to use for subscribing to metadata event streams.
func WithSubscription(subscription string) func(*Option) {
	return func(o *Option) {
		o.subscription = strings.TrimSpace(subscription)
	}
}

// WithTimeOffsetBegin sets the timestamp from which to start watching for events.
func WithTimeOffsetBegin(timestamp time.Time) func(*Option) {
	return func(o *Option) {
		o.timeOffsetBegin = timestamp
	}
}

// WithTimeOffsetEnd sets the timestamp for which stop watching for events. If the timestamp is less than or equal to
// the beginning offset, the zero-value (0001-01-01 00:00:00 +0000 UTC) will be used, effectively consuming all new
// events greater than or equal to the beginning offset.
func WithTimeOffsetEnd(timestamp time.Time) func(*Option) {
	return func(o *Option) {
		if !timestamp.IsZero() && timestamp.After(o.timeOffsetBegin) {
			o.timeOffsetEnd = timestamp
		}
	}
}

// WithWriterConcurrency sets the concurrency for reading metadata chunks from storage events. Default is runtime.NumCPU().
//
// If the total size in bytes of chunks for an event exceeds the value set using WithWriterChunkBufferSize, this option
// will be ignored.
func WithWriterConcurrency(concurrency int) func(*Option) {
	return func(o *Option) {
		o.writerConcurrency = concurrency
	}
}

// WithWriterChunkBufferSize sets the maximum cumulative size in bytes of chunks that can be read when processing
// storage events. Default is 1GiB.
//
// If the total size in bytes of chunks for an event exceeds the buffer size, they will be read as a stream.
func WithWriterChunkBufferSize(size int64) func(*Option) {
	return func(o *Option) {
		o.writerChunkBufferSize = size
	}
}
