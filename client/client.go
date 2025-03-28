package client

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/transientvariable/config"
	"github.com/transientvariable/configpath"
	"github.com/transientvariable/log"
	"github.com/transientvariable/net/grpc"

	"github.com/cenkalti/backoff/v4"

	gogrpc "google.golang.org/grpc"
)

const (
	HTTPURIScheme               = "http"
	grpcConnRetries             = 5
	clusterLocalHostname        = "0.0.0.0"
	readinessProbeMaxRetries    = 10
	readinessProbeRetryInterval = 3 * time.Second
)

var (
	GID = int32(os.Getgid())
	UID = int32(os.Getuid())
)

// Client defines the behavior for a SeaweedFS API client.
type Client interface {
	Addr() url.URL
	Config() (map[string]any, error)
	GRPCAddr() string
	ID() ID
	Name() string
	Ready(ctx context.Context) (any, error)
	String() string
}

// NewClientConn creates a new gRPC connection using the provided SeaweedFS API Client.
func NewClientConn(client Client) (*gogrpc.ClientConn, error) {
	if client == nil {
		return nil, &Error{Err: errors.New("connection attributes are missing or invalid")}
	}

	addr := client.GRPCAddr()

	log.Debug("[client] creating gRPC connection",
		log.String("name", client.Name()),
		log.String("target", addr))

	grpcOpts := []func(*grpc.Option){
		grpc.WithKeepAlivePermitWithoutStream(config.BoolMustResolve(configpath.GRPCKeepAlivePermitWithoutStream)),
		grpc.WithMessageSizeMaxReceive(config.SizeMustResolve(configpath.GRPCMessageSizeMaxReceive)),
		grpc.WithMessageSizeMaxSend(config.SizeMustResolve(configpath.GRPCMessageSizeMaxSend)),
		grpc.WithMinKeepAliveTime(config.DurationMustResolve(configpath.GRPCKeepAliveTime)),
		grpc.WithMaxKeepAliveTimeout(config.DurationMustResolve(configpath.GRPCKeepAliveTimeout)),
		grpc.WithSOCKS5Enabled(config.BoolMustResolve(configpath.SOCKS5Enable)),
		grpc.WithTLSCertFilePath(config.ValueMustResolve(configpath.GRPCSecurityTLSCertFile)),
		grpc.WithTLSKeyFilePath(config.ValueMustResolve(configpath.GRPCSecurityTLSKeyFile)),
		grpc.WithTLSEnabled(config.BoolMustResolve(configpath.GRPCSecurityTLSEnable)),
	}

	var conn *gogrpc.ClientConn
	err := backoff.Retry(func() error {
		c, err := grpc.New(addr, grpcOpts...)
		if err != nil {
			return &Error{Err: fmt.Errorf("could not create gRPC connection for target: %s: %w", addr, err)}
		}
		conn = c
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(grpcConnRetries)))
	return conn, err
}

// EncodeAddrs ...
func EncodeAddrs(addrs ...url.URL) []url.URL {
	for i, a := range addrs {
		addrs[i] = EncodeAddr(a)
	}
	return addrs
}

// EncodeAddr ...
func EncodeAddr(addr url.URL) url.URL {
	local, err := config.Bool(configpath.SeaweedFSClusterLocal)
	if err != nil {
		local = false
	}

	if local {
		if p := addr.Port(); p != "" {
			addr.Host = clusterLocalHostname + ":" + p
		} else {
			addr.Host = clusterLocalHostname
		}
	}
	return addr
}

// Ready is a readiness probe for a SeaweedFS API Client.
func Ready(ctx context.Context, client Client) (any, error) {
	addr := client.GRPCAddr()

	log.Info("[client] connecting to server",
		log.String("name", client.Name()),
		log.String("target", addr))

	ready := make(chan any)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		pollReadyState(ctx, client, ready)
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(ready)
	}()

	retries := 0
	for resp := range ready {
		if retries > readinessProbeMaxRetries {
			break
		}

		if _, ok := resp.(error); ok {
			log.Info("[client] waiting to connect to server",
				log.String("name", client.Name()),
				log.String("address", addr))
			retries++
			continue
		}
		return resp, nil
	}
	return nil, &Error{Err: errors.New("maximum number of gRPC connection retries exceeded")}
}

func pollReadyState(ctx context.Context, client Client, ready chan<- any) {
	ticker := backoff.NewTicker(backoff.NewConstantBackOff(readinessProbeRetryInterval))
	for {
		select {
		case <-ticker.C:
			r, err := client.Ready(ctx)
			if err != nil {
				ready <- &Error{Err: fmt.Errorf("could not retrieve configuration: %w", err)}
				continue
			}
			ready <- r
		case <-ctx.Done():
			return
		}
	}
}
