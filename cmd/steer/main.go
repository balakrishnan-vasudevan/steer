// steer is our load balancer. Phase 4: least connections, so busy or slow
// backends get less traffic.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/balakrishnan-vasudevan/steer/internal/pool"
)

func main() {
	listen := flag.String("listen", ":8080", "address the LB listens on")
	backends := flag.String("backends", "http://localhost:8081",
		"comma-separated backend URLs, e.g. http://localhost:8081,http://localhost:8082")
	strategy := flag.String("strategy", string(pool.RoundRobin),
		"how to pick a backend: round-robin or least-conn")
	healthPath := flag.String("health-path", "/healthz", "path to poll on each backend")
	healthInterval := flag.Duration("health-interval", 2*time.Second, "time between health checks")
	healthTimeout := flag.Duration("health-timeout", time.Second, "how long a health check may take")
	flag.Parse()

	p, err := pool.New(strings.Split(*backends, ","), pool.Strategy(*strategy))
	if err != nil {
		log.Fatal(err)
	}

	go p.HealthCheck(context.Background(), *healthPath, *healthInterval, *healthTimeout)

	for _, b := range p.Backends() {
		log.Printf("backend: %s", b.URL)
	}
	log.Printf("steer listening on %s (strategy: %s)", *listen, *strategy)
	log.Fatal(http.ListenAndServe(*listen, p))
}
