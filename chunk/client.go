package chunk

import (
	"sync"

	"github.com/transientvariable/net-go/http"
	"github.com/transientvariable/support-go"

	gohttp "net/http"
)

var (
	c    *gohttp.Client
	once sync.Once
)

func httpClient() *gohttp.Client {
	once.Do(func() {
		t := http.DefaultTransport()
		t.ReadBufferSize = support.MiB
		t.WriteBufferSize = support.MiB
		c = http.NewClient()
		c.Transport = t
	})
	return c
}
