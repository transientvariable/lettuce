package chunk

import (
	"sync"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/anchor/net/http"

	gohttp "net/http"
)

var (
	c    *gohttp.Client
	once sync.Once
)

func httpClient() *gohttp.Client {
	once.Do(func() {
		t := http.DefaultTransport()
		t.ReadBufferSize = anchor.MiB
		t.WriteBufferSize = anchor.MiB
		c = http.NewClient()
		c.Transport = t
	})
	return c
}
