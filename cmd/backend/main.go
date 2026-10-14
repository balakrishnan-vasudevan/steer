// backend is a toy HTTP server we put behind the load balancer.
// Run several copies on different ports to simulate a fleet.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	port := flag.Int("port", 8081, "port to listen on")
	delay := flag.Duration("delay", 0, "artificial latency per request (e.g. 200ms)")
	flag.Parse()

	name := fmt.Sprintf("backend-%d", *port)

	mux := http.NewServeMux()

	// The main endpoint: say who answered, so we can see where the LB sent us.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(*delay) // used in phase 4 to make one backend slow
		fmt.Fprintf(w, "hello from %s (path=%s)\n", name, r.URL.Path)
	})

	// Health endpoint: the LB's health checker will poll this in phase 3.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("%s listening on %s (delay=%s)", name, addr, *delay)
	log.Fatal(http.ListenAndServe(addr, mux))
}
