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

	// active counts requests this backend is handling right now. Every
	// request changes it on the way in and out, so it's atomic too.
	active atomic.Int64
}

// Active reports how many requests this backend is handling right now.
func (b *Backend) Active() int64 {
	return b.active.Load()
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

// Strategy is how the pool picks a backend for each request.
type Strategy string

const (
	// RoundRobin takes the backends in turn, regardless of how busy they are.
	RoundRobin Strategy = "round-robin"
	// LeastConnections picks the backend with the fewest requests in flight.
	LeastConnections Strategy = "least-conn"
)

// Pool is a fixed list of backends, skipping any that are down. It is safe
// to use from many goroutines at once.
type Pool struct {
	backends []*Backend
	strategy Strategy
	next     atomic.Uint64 // how many picks have been made so far
}

// New builds a pool from backend URLs like "http://localhost:8081".
// Backends start out alive; the health checker corrects that within one check.
func New(rawURLs []string, strategy Strategy) (*Pool, error) {
	if len(rawURLs) == 0 {
		return nil, errors.New("pool needs at least one backend")
	}
	if strategy != RoundRobin && strategy != LeastConnections {
		return nil, fmt.Errorf("unknown strategy %q (want %q or %q)", strategy, RoundRobin, LeastConnections)
	}

	p := &Pool{strategy: strategy}
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
	if p.strategy == RoundRobin {
		return available[n%uint64(len(available))]
	}

	// Least connections: the backends with the fewest requests in flight.
	// Read each count once, because they change while we look.
	counts := make([]int64, len(available))
	fewest := int64(-1)
	for i, b := range available {
		counts[i] = b.Active()
		if fewest < 0 || counts[i] < fewest {
			fewest = counts[i]
		}
	}

	// Several backends are often tied (all idle, say). Take turns among the
	// tied ones. Just picking the first would hand every tie to whichever
	// backend comes first in the list.
	tied := make([]*Backend, 0, len(available))
	for i, b := range available {
		if counts[i] == fewest {
			tied = append(tied, b)
		}
	}
	return tied[n%uint64(len(tied))]
}

// Backends returns the backends in the pool, in the order they were given.
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
