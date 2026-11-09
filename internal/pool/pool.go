// Package pool holds the set of backends steer can route to and decides
// which one gets the next request.
package pool

import (
	"errors"
	"fmt"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
)

// Backend is one upstream server, plus the proxy that forwards to it.
type Backend struct {
	URL   *url.URL
	Proxy *httputil.ReverseProxy
}

// Pool is a fixed list of backends picked in round-robin order.
// It is safe to call Next from many goroutines at once.
type Pool struct {
	backends []*Backend
	next     atomic.Uint64 // how many picks have been made so far
}

// New builds a pool from backend URLs like "http://localhost:8081".
func New(rawURLs []string) (*Pool, error) {
	if len(rawURLs) == 0 {
		return nil, errors.New("pool needs at least one backend")
	}

	p := &Pool{}
	for _, raw := range rawURLs {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("bad backend URL %q: %w", raw, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("backend URL %q needs a scheme and host, e.g. http://localhost:8081", raw)
		}
		p.backends = append(p.backends, &Backend{
			URL:   u,
			Proxy: httputil.NewSingleHostReverseProxy(u),
		})
	}
	return p, nil
}

// Next returns the backend that should handle the next request.
func (p *Pool) Next() *Backend {
	// Add is atomic: every caller gets a unique number, even when
	// thousands of requests arrive at the same instant. We subtract 1 so
	// the first pick is index 0.
	n := p.next.Add(1) - 1
	return p.backends[n%uint64(len(p.backends))]
}

// Backends returns the backends in the pool, in round-robin order.
func (p *Pool) Backends() []*Backend {
	return p.backends
}
