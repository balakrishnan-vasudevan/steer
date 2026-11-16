package pool

import (
	"context"
	"log"
	"net/http"
)

// failureKey is the context key ServeHTTP uses to find out whether a proxy
// attempt failed. An unexported type means no other package can collide with it.
type failureKey struct{}

// ServeHTTP forwards a request to a healthy backend. If the backend can't be
// reached, it's taken out of rotation and, when that's safe, the request is
// retried on another one.
func (p *Pool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tried := map[*Backend]bool{}

	for {
		b := p.Next(tried)
		if b == nil {
			log.Printf("%s %s -> no healthy backends (tried %d)", r.Method, r.URL.Path, len(tried))
			http.Error(w, "no healthy backends", http.StatusServiceUnavailable)
			return
		}
		tried[b] = true

		log.Printf("%s %s -> %s", r.Method, r.URL.Path, b.URL)
		var failed error
		ctx := context.WithValue(r.Context(), failureKey{}, &failed)
		b.Proxy.ServeHTTP(w, r.WithContext(ctx))
		if failed == nil {
			return
		}

		// If the client hung up, the backend did nothing wrong. Don't take a
		// healthy backend out of rotation because someone closed a tab.
		if r.Context().Err() != nil {
			log.Printf("%s %s -> client went away", r.Method, r.URL.Path)
			return
		}

		log.Printf("%s %s -> %s failed: %v", r.Method, r.URL.Path, b.URL, failed)
		b.SetAlive(false, "request failed")

		if !retryable(r) {
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
	}
}

// retryable reports whether a request can safely be sent a second time.
// The backend may have received and acted on the first attempt before the
// connection failed, so only methods with no side effects qualify. A request
// with a body is out too: the first attempt has already consumed it.
func retryable(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return r.ContentLength == 0
	}
	return false
}
