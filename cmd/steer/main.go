// steer is our load balancer. Phase 2: round robin across several backends.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/balakrishnan-vasudevan/steer/internal/pool"
)

func main() {
	listen := flag.String("listen", ":8080", "address the LB listens on")
	backends := flag.String("backends", "http://localhost:8081",
		"comma-separated backend URLs, e.g. http://localhost:8081,http://localhost:8082")
	flag.Parse()

	p, err := pool.New(strings.Split(*backends, ","))
	if err != nil {
		log.Fatal(err)
	}

	// The routing decision lives here: ask the pool for a backend, then
	// hand the request to that backend's proxy.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := p.Next()
		log.Printf("%s %s -> %s", r.Method, r.URL.Path, b.URL)
		b.Proxy.ServeHTTP(w, r)
	})

	for _, b := range p.Backends() {
		log.Printf("backend: %s", b.URL)
	}
	log.Printf("steer listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, handler))
}
