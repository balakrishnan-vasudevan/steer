// steer is our load balancer. Phase 1: forward everything to a single backend.
package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func main() {
	listen := flag.String("listen", ":8080", "address the LB listens on")
	backend := flag.String("backend", "http://localhost:8081", "backend URL to forward to")
	flag.Parse()

	target, err := url.Parse(*backend)
	if err != nil {
		log.Fatalf("bad backend URL %q: %v", *backend, err)
	}

	// ReverseProxy does the heavy lifting: for each incoming request it
	// rewrites the URL to point at target, sends it upstream, and streams
	// the response back to the client.
	proxy := httputil.NewSingleHostReverseProxy(target)

	// Wrap the proxy so we can log each request. This wrapper is where the
	// "which backend?" decision will live from phase 2 onwards.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s -> %s", r.Method, r.URL.Path, target)
		proxy.ServeHTTP(w, r)
	})

	log.Printf("LB listening on %s, forwarding to %s", *listen, target)
	log.Fatal(http.ListenAndServe(*listen, handler))
}
