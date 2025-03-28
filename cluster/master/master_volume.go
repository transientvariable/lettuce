package master

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/transientvariable/lettuce/client"
	"github.com/transientvariable/lettuce/pb/master_pb"
	"github.com/transientvariable/log"
	"github.com/transientvariable/support"

	"google.golang.org/grpc/status"

	gofs "io/fs"
)

// FindVolumes returns the list of volume server URLs that have data associated with provided collection and file ID.
func (m *Master) FindVolumes(ctx context.Context, collection string, fileID string) ([]url.URL, error) {
	log.Trace("[master] findVolumes", log.String("collection", collection), log.String("file_id", fileID))

	f := strings.Split(fileID, ",")
	if len(f) != 2 {
		return nil, errors.New(fmt.Sprintf("master: invalid file id: %s", fileID))
	}

	resp, err := m.PB().LookupVolume(ctx, &master_pb.LookupVolumeRequest{
		Collection:      collection,
		VolumeOrFileIds: []string{f[0]},
	})
	if err != nil {
		s, ok := status.FromError(err)
		if !ok {
			return nil, err
		}
		return nil, fmt.Errorf("master: %w", errors.New(s.Message()))
	}

	if len(resp.VolumeIdLocations) == 0 {
		return nil, fmt.Errorf("master: %w", gofs.ErrNotExist)
	}

	var addrs []url.URL
	for _, l := range resp.GetVolumeIdLocations()[0].GetLocations() {
		if support.URISchemePattern.MatchString(l.GetUrl()) {
			u, err := url.Parse(l.GetUrl())
			if err != nil {
				return nil, fmt.Errorf("master: %w", err)
			}
			addrs = append(addrs, *u)
		}
		addrs = append(addrs, url.URL{Scheme: client.HTTPURIScheme, Host: l.GetUrl()})
	}

	log.Trace("[master] findVolumes",
		log.String("collection", collection),
		log.String("file_id", fileID),
		log.Int("volumes_found", len(addrs)))

	return client.EncodeAddrs(addrs...), nil
}

// VolumeAddresses returns the list of all volume server addresses known by the Master server API client.
func (m *Master) VolumeAddresses(ctx context.Context) ([]url.URL, error) {
	vols, err := m.PB().VolumeList(ctx, &master_pb.VolumeListRequest{})
	if err != nil {
		return nil, err
	}

	var addrs []url.URL
	for _, dc := range vols.GetTopologyInfo().GetDataCenterInfos() {
		for _, r := range dc.GetRackInfos() {
			for _, n := range r.GetDataNodeInfos() {
				// TODO: Need a better way to determine URI scheme used for volume URLs.
				addrs = append(addrs, url.URL{Scheme: "http", Host: n.Id})
			}
		}
	}
	encodedAddrs := client.EncodeAddrs(addrs...)

	log.Trace(fmt.Sprintf("[master] volumeAddresses: %s\n", support.ToJSONFormatted(encodedAddrs)))

	return encodedAddrs, nil
}
