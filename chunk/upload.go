package chunk

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/anchor/net/http"
	"github.com/transientvariable/lettuce/pb/filer_pb"

	json "github.com/json-iterator/go"
	gohttp "net/http"
)

const (
	CookieSize    = 4
	NeedleIdSize  = 8
	NeedleIdEmpty = 0
)

// UploadResult a container for representing the result of a chunked upload operation.
type UploadResult struct {
	CipherKey  []byte `json:"cipherKey,omitempty"`
	ContentMd5 string `json:"contentMd5,omitempty"`
	Error      string `json:"error,omitempty"`
	ETag       string `json:"eTag,omitempty"`
	GZip       uint32 `json:"gzip,omitempty"`
	MimeType   string `json:"mime,omitempty"`
	Name       string `json:"name,omitempty"`
	Size       uint32 `json:"size,omitempty"`
}

// FileChunk returns a filer_pb.FileChunk for the UploadResult using the provided properties.
func (u UploadResult) FileChunk(fileId string, offset int64, tsNs int64) (*filer_pb.FileChunk, error) {
	fid, err := parseFID(fileId)
	if err != nil {
		return nil, err
	}
	return &filer_pb.FileChunk{
		CipherKey:    u.CipherKey,
		ETag:         u.ContentMd5,
		Fid:          fid,
		FileId:       fileId,
		IsCompressed: u.GZip > 0,
		ModifiedTsNs: tsNs,
		Offset:       offset,
		Size:         uint64(u.Size),
	}, nil
}

// String returns a string representation of the UploadResult.
func (u UploadResult) String() string {
	return string(anchor.ToJSONFormatted(u))
}

func decodeUploadResponse(resp *gohttp.Response) (UploadResult, error) {
	eTag := resp.Header.Get(http.HeaderETag)
	if strings.HasPrefix(eTag, "\"") && strings.HasSuffix(eTag, "\"") {
		eTag = eTag[1 : len(eTag)-1]
	}

	if resp.StatusCode == gohttp.StatusNoContent {
		return UploadResult{ETag: eTag}, nil
	}

	var r UploadResult
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return r, err
	}
	r.ETag = eTag
	r.ContentMd5 = resp.Header.Get(http.HeaderContentMD5)
	return r, nil
}

func parseFID(fid string) (*filer_pb.FileId, error) {
	vid, needleKeyCookie, err := splitVolumeId(fid)
	if err != nil {
		return nil, err
	}

	volumeId, err := strconv.ParseUint(vid, 10, 64)
	if err != nil {
		return nil, err
	}

	nid, cookie, err := parseNeedleIdCookie(needleKeyCookie)
	if err != nil {
		return nil, err
	}
	return &filer_pb.FileId{
		Cookie:   cookie,
		FileKey:  nid,
		VolumeId: uint32(volumeId),
	}, nil
}

func parseNeedleIdCookie(hash string) (uint64, uint32, error) {
	if len(hash) <= CookieSize*2 {
		return NeedleIdEmpty, 0, errors.New("needle hash key too short")
	}

	if len(hash) > (NeedleIdSize+CookieSize)*2 {
		return NeedleIdEmpty, 0, fmt.Errorf("needle hash key too long")
	}

	split := len(hash) - CookieSize*2
	needleId, err := strconv.ParseUint(hash[:split], 16, 64)
	if err != nil {
		return NeedleIdEmpty, 0, fmt.Errorf("invalid needle Id format: %w", err)
	}

	cookie, err := strconv.ParseUint(hash[split:], 16, 32)
	if err != nil {
		return NeedleIdEmpty, 0, fmt.Errorf("invalid cookie format: %w", err)
	}
	return needleId, uint32(cookie), nil
}

func splitVolumeId(fid string) (string, string, error) {
	commaIndex := strings.Index(fid, ",")
	if commaIndex <= 0 {
		return "", "", fmt.Errorf("invalid fid format")
	}
	return fid[:commaIndex], fid[commaIndex+1:], nil
}
