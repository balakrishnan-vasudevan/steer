// Package pool holds the set of backends steer can route to, keeps track of
// which ones are healthy, and decides which one gets the next request.
package pool

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
)

// Backend is one upstream server, plus the proxy that forwards to it.
type Backend struct {
	URL   *url.URL
	Proxy *httputil.ReverseProxy

	// alive is read by every request and written by the health checker,
	// often at the same moment, so it has to be atomic.
	alive atomic.Bool
}

// IsAlive reports whether this backend is currently in rotation.
func (b *Backend) IsAlive() bool {
	return b.alive.Load()
}

// SetAlive puts the backend in or out of rotation, and logs when that changes.
func (b *Backend) SetAlive(alive bool, reason string) {
	// Swap returns the previous value, so two goroutines reporting the same
	// failure produce one log line, not two.
	if b.alive.Swap(alive) == alive {
		return
	}
	state := "DOWN"
	if alive {
		state = "UP"
	}
	log.Printf("backend %s is %s (%s)", b.URL, state, reason)
}

// Pool is a fixed list of backends picked in round-robin order, skipping any
// that are down. It is safe to use from many goroutines at once.
type Pool struct {
	backends []*Backend
	next     atomic.Uint64 // how many picks have been made so far
}

// New builds a pool from backend URLs like "http://localhost:8081".
// Backends start out alive; the health checker corrects that within one check.
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

		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.ErrorHandler = recordFailure

		b := &Backend{URL: u, Proxy: proxy}
		b.alive.Store(true)
		p.backends = append(p.backends, b)
	}
	return p, nil
}

// Next returns the backend that should handle the next request, ignoring any
// in tried, or nil if no backend is available.
func (p *Pool) Next(tried map[*Backend]bool) *Backend {
	// Take one snapshot of who's available and pick from it. Counting and
	// then scanning separately could disagree if a backend changes state in
	// between, and skipping forward from a dead backend would hand its whole
	// share to whichever backend happens to come after it.
	available := make([]*Backend, 0, len(p.backends))
	for _, b := range p.backends {
		if b.IsAlive() && !tried[b] {
			available = append(available, b)
		}
	}
	if len(available) == 0 {
		return nil
	}

	n := p.next.Add(1) - 1
	return available[n%uint64(len(available))]
}

// Backends returns the backends in the pool, in round-robin order.
func (p *Pool) Backends() []*Backend {
	return p.backends
}

// recordFailure replaces ReverseProxy's default error handler, which would
// immediately send the client a 502. Writing nothing here leaves the response
// untouched, so ServeHTTP can still try another backend.
func recordFailure(w http.ResponseWriter, r *http.Request, err error) {
	if failed, ok := r.Context().Value(failureKey{}).(*error); ok {
		*failed = err
		return
	}
	http.Error(w, "bad gateway", http.StatusBadGateway)
}
