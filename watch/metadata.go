package watch

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/transientvariable/lettuce/cluster/filer"
	"github.com/transientvariable/lettuce/pb/filer_pb"
	"github.com/transientvariable/log"
)

type metadataClient struct {
	client filer_pb.SeaweedFiler_SubscribeMetadataClient
	req    *filer_pb.SubscribeMetadataRequest
	mutex  sync.RWMutex
}

func newMetadataClient(ctx context.Context, filer *filer.Filer, options *Option) (*metadataClient, error) {
	mc := &metadataClient{req: prepareMetadataRequest(options)}

	switch options.subscription {
	case "local":
		c, err := filer.PB().SubscribeLocalMetadata(ctx, mc.req)
		if err != nil {
			return nil, err
		}
		mc.client = c
		break
	case "remote":
		c, err := filer.PB().SubscribeMetadata(ctx, mc.req)
		if err != nil {
			return nil, err
		}
		mc.client = c
		break
	default:
		return nil, errors.New(fmt.Sprintf("seaweedfs_watcher: metadata subscription type is unsupported: %s", options.subscription))
	}
	return mc, nil
}

func (c *metadataClient) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	resp, err := c.client.Recv()
	if err != nil {
		return nil, err
	}

	c.req.SinceNs = resp.TsNs
	return resp, nil
}

func (c *metadataClient) Close() error {
	if c.client != nil {
		return c.client.CloseSend()
	}
	return nil
}

func prepareMetadataRequest(options *Option) *filer_pb.SubscribeMetadataRequest {
	log.Debug(fmt.Sprintf("[seaweedfs:watcher] preparing metadata cluster using options:\n%s", options))

	request := &filer_pb.SubscribeMetadataRequest{
		ClientId:     options.clientID,
		ClientName:   options.clientName,
		PathPrefix:   options.pathPrefix,
		PathPrefixes: options.pathPrefixes,
		Signature:    options.signature,
	}

	if !options.timeOffsetBegin.IsZero() {
		request.SinceNs = options.timeOffsetBegin.UnixNano()
	}

	if !options.timeOffsetEnd.IsZero() {
		request.UntilNs = options.timeOffsetEnd.UnixNano()
	}
	return request
}
