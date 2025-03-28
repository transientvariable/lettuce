//go:build integration

package seaweedfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/transientvariable/log-go"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/webdav"

	gopath "path"
)

func TestPrefix(t *testing.T) {
	t.Skip("Test is a WIP, use only for local testing")

	const dst, blah = "Destination", "blah blah blah"

	// createLockBody comes from the example in Section 9.10.7.
	const createLockBody = `<?xml version="1.0" encoding="utf-8" ?>
		<D:lockinfo xmlns:D='DAV:'>
			<D:lockscope><D:exclusive/></D:lockscope>
			<D:locktype><D:write/></D:locktype>
			<D:owner>
				<D:href>http://example.org/~ejw/contact.html</D:href>
			</D:owner>
		</D:lockinfo>
	`

	do := func(method, urlStr string, body string, wantStatusCode int, headers ...string) (http.Header, error) {
		var bodyReader io.Reader
		if body != "" {
			bodyReader = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, urlStr, bodyReader)
		if err != nil {
			return nil, err
		}
		for len(headers) >= 2 {
			req.Header.Add(headers[0], headers[1])
			headers = headers[2:]
		}
		res, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		defer func(Body io.ReadCloser) {
			if err := Body.Close(); err != nil {
				log.Error("[webdav:test]", log.Err(err))
			}
		}(res.Body)
		if res.StatusCode != wantStatusCode {
			return nil, fmt.Errorf("got status code %d, want %d", res.StatusCode, wantStatusCode)
		}
		return res.Header, nil
	}

	prefixes := []string{
		"/",
		"/a/",
		"/a/b/",
		"/a/b/c/",
	}

	fsys, err := New()
	assert.NoError(t, err)
	wdfs, err := NewWebDAV(fsys)
	assert.NoError(t, err)
	defer func(fs *WebDAV) {
		assert.NoError(t, fs.Close())
	}(wdfs)

	//ctx := context.Background()

	for _, prefix := range prefixes {
		h := &webdav.Handler{
			FileSystem: wdfs,
			LockSystem: webdav.NewMemLS(),
		}
		mux := http.NewServeMux()
		if prefix != "/" {
			h.Prefix = prefix
		}
		mux.Handle(prefix, h)
		srv := httptest.NewServer(mux)
		defer srv.Close()

		// The script is:
		//	MKCOL /a
		//	MKCOL /a/b
		//	PUT   /a/b/c
		//	COPY  /a/b/c /a/b/d
		//	MKCOL /a/b/e
		//	MOVE  /a/b/d /a/b/e/f
		//	LOCK  /a/b/e/g
		//	PUT   /a/b/e/g
		// which should yield the (possibly stripped) filenames /a/b/c,
		// /a/b/e/f and /a/b/e/g, plus their parent directories.

		wantA := map[string]int{
			"/":       http.StatusCreated,
			"/a/":     http.StatusMovedPermanently,
			"/a/b/":   http.StatusNotFound,
			"/a/b/c/": http.StatusNotFound,
		}[prefix]
		if _, err := do("MKCOL", srv.URL+"/a", "", wantA); err != nil {
			t.Errorf("prefix=%-9q MKCOL /a: %v", prefix, err)
			continue
		}

		wantB := map[string]int{
			"/":       http.StatusCreated,
			"/a/":     http.StatusCreated,
			"/a/b/":   http.StatusMovedPermanently,
			"/a/b/c/": http.StatusNotFound,
		}[prefix]
		if _, err := do("MKCOL", srv.URL+"/a/b", "", wantB); err != nil {
			t.Errorf("prefix=%-9q MKCOL /a/b: %v", prefix, err)
			continue
		}

		wantC := map[string]int{
			"/":       http.StatusCreated,
			"/a/":     http.StatusCreated,
			"/a/b/":   http.StatusCreated,
			"/a/b/c/": http.StatusMovedPermanently,
		}[prefix]
		if _, err := do("PUT", srv.URL+"/a/b/c", blah, wantC); err != nil {
			t.Errorf("prefix=%-9q PUT /a/b/c: %v", prefix, err)
			continue
		}

		wantD := map[string]int{
			"/":       http.StatusCreated,
			"/a/":     http.StatusCreated,
			"/a/b/":   http.StatusCreated,
			"/a/b/c/": http.StatusMovedPermanently,
		}[prefix]
		if _, err := do("COPY", srv.URL+"/a/b/c", "", wantD, dst, srv.URL+"/a/b/d"); err != nil {
			t.Errorf("prefix=%-9q COPY /a/b/c /a/b/d: %v", prefix, err)
			continue
		}

		wantE := map[string]int{
			"/":       http.StatusCreated,
			"/a/":     http.StatusCreated,
			"/a/b/":   http.StatusCreated,
			"/a/b/c/": http.StatusNotFound,
		}[prefix]
		if _, err := do("MKCOL", srv.URL+"/a/b/e", "", wantE); err != nil {
			t.Errorf("prefix=%-9q MKCOL /a/b/e: %v", prefix, err)
			continue
		}

		wantF := map[string]int{
			"/":       http.StatusCreated,
			"/a/":     http.StatusCreated,
			"/a/b/":   http.StatusCreated,
			"/a/b/c/": http.StatusNotFound,
		}[prefix]
		if _, err := do("MOVE", srv.URL+"/a/b/d", "", wantF, dst, srv.URL+"/a/b/e/f"); err != nil {
			t.Errorf("prefix=%-9q MOVE /a/b/d /a/b/e/f: %v", prefix, err)
			continue
		}

		var lockToken string
		wantG := map[string]int{
			"/":       http.StatusCreated,
			"/a/":     http.StatusCreated,
			"/a/b/":   http.StatusCreated,
			"/a/b/c/": http.StatusNotFound,
		}[prefix]
		if h, err := do("LOCK", srv.URL+"/a/b/e/g", createLockBody, wantG); err != nil {
			t.Errorf("prefix=%-9q LOCK /a/b/e/g: %v", prefix, err)
			continue
		} else {
			lockToken = h.Get("Lock-Token")
		}

		ifHeader := fmt.Sprintf("<%s/a/b/e/g> (%s)", srv.URL, lockToken)
		wantH := map[string]int{
			"/":       http.StatusCreated,
			"/a/":     http.StatusCreated,
			"/a/b/":   http.StatusCreated,
			"/a/b/c/": http.StatusNotFound,
		}[prefix]
		if _, err := do("PUT", srv.URL+"/a/b/e/g", blah, wantH, "If", ifHeader); err != nil {
			t.Errorf("prefix=%-9q PUT /a/b/e/g: %v", prefix, err)
			continue
		}

		//got, err := find(ctx, nil, fs, prefix)
		//if err != nil {
		//	t.Errorf("prefix=%-9q find: %v", prefix, err)
		//	continue
		//}
		//sort.Strings(got)
		//want := map[string][]string{
		//	"/":       {"/", "/a", "/a/b", "/a/b/c", "/a/b/e", "/a/b/e/f", "/a/b/e/g"},
		//	"/a/":     {"/", "/b", "/b/c", "/b/e", "/b/e/f", "/b/e/g"},
		//	"/a/b/":   {"/", "/c", "/e", "/e/f", "/e/g"},
		//	"/a/b/c/": {"/"},
		//}[prefix]
		//if !reflect.DeepEqual(got, want) {
		//	t.Errorf("prefix=%-9q find:\ngot  %v\nwant %v", prefix, got, want)
		//	continue
		//}

		//for _, p := range []string{"/a/", "/b/", "/c/"} {
		//	assert.NoError(t, fs.RemoveAll(ctx, p))
		//}
	}
}

// find appends to ss the names of the named file and its children. It is
// analogous to the Unix find command.
//
// The returned strings are not guaranteed to be in any particular order.
func find(ctx context.Context, ss []string, fs webdav.FileSystem, name string) ([]string, error) {
	stat, err := fs.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	ss = append(ss, name)
	if stat.IsDir() {
		f, err := fs.OpenFile(ctx, name, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer func(f webdav.File) {
			if err := f.Close(); err != nil {
				log.Error("[webdav:test]", log.Err(err))
			}
		}(f)
		children, err := f.Readdir(-1)
		if err != nil {
			return nil, err
		}
		for _, c := range children {
			ss, err = find(ctx, ss, fs, gopath.Join(name, c.Name()))
			if err != nil {
				return nil, err
			}
		}
	}
	return ss, nil
}
