package pool

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var urls = []string{
	"http://localhost:8081",
	"http://localhost:8082",
	"http://localhost:8083",
}

func TestNextRotatesInOrder(t *testing.T) {
	p, err := New(urls, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}

	// Two full laps: 8081, 8082, 8083, 8081, 8082, 8083.
	for i := 0; i < 6; i++ {
		want := urls[i%len(urls)]
		if got := p.Next(nil).URL.String(); got != want {
			t.Errorf("pick %d: got %s, want %s", i, got, want)
		}
	}
}

// Many goroutines picking at once must still spread picks perfectly evenly.
// Run with `go test -race` to also check for data races.
func TestNextIsEvenUnderConcurrency(t *testing.T) {
	p, err := New(urls, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 100
	const picksEach = 300 // 30,000 picks total, divisible by 3

	var mu sync.Mutex
	counts := map[string]int{}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := map[string]int{}
			for i := 0; i < picksEach; i++ {
				local[p.Next(nil).URL.Host]++
			}
			mu.Lock()
			for host, n := range local {
				counts[host] += n
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	want := goroutines * picksEach / len(urls)
	for _, u := range p.Backends() {
		if got := counts[u.URL.Host]; got != want {
			t.Errorf("%s got %d picks, want exactly %d", u.URL.Host, got, want)
		}
	}
}

func TestNewRejectsBadInput(t *testing.T) {
	cases := map[string][]string{
		"empty list":  {},
		"no scheme":   {"localhost:8081"},
		"bad URL":     {"http://[::1"},
		"one bad one": {"http://localhost:8081", "nope"},
	}
	for name, in := range cases {
		if _, err := New(in, RoundRobin); err == nil {
			t.Errorf("%s: expected an error for %v", name, in)
		}
	}
}

// A dead backend's traffic must be shared evenly, not all dumped on
// whichever backend happens to come after it in the list.
func TestNextSharesDeadBackendsTrafficEvenly(t *testing.T) {
	p, err := New(urls, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	p.Backends()[1].SetAlive(false, "test")

	counts := map[string]int{}
	for i := 0; i < 3000; i++ {
		counts[p.Next(nil).URL.Host]++
	}
	if counts["localhost:8082"] != 0 {
		t.Errorf("dead backend got %d picks", counts["localhost:8082"])
	}
	if counts["localhost:8081"] != 1500 || counts["localhost:8083"] != 1500 {
		t.Errorf("uneven split: %v", counts)
	}
}

func TestNextReturnsNilWhenNothingAvailable(t *testing.T) {
	p, err := New(urls[:2], RoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	p.Backends()[0].SetAlive(false, "test")
	if b := p.Next(map[*Backend]bool{p.Backends()[1]: true}); b != nil {
		t.Errorf("got %s, want nil", b.URL)
	}
}

// The health checker writes alive while requests read it. This test exists
// for `go test -race`.
func TestAliveIsSafeUnderConcurrency(t *testing.T) {
	p, err := New(urls, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				p.Next(nil)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				p.Backends()[i%3].alive.Store(i%2 == 0)
			}
		}()
	}
	wg.Wait()
}

// startBackend runs a test server that says its name and counts its requests.
func startBackend(t *testing.T, name string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		io.WriteString(w, name)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// deadURL returns the address of a server that has already shut down, so
// connecting to it fails straight away.
func deadURL() string {
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close()
	return u
}

func do(t *testing.T, h http.Handler, method string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/x", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// The point of phase 3: the client never finds out a backend died.
func TestRetriesGetOnAnotherBackend(t *testing.T) {
	live, _ := startBackend(t, "live")
	p, err := New([]string{deadURL(), live.URL}, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}

	// Round robin starts at the dead backend, so this request hits it first.
	rec := do(t, p, http.MethodGet, nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "live" {
		t.Errorf("got %d %q, want 200 \"live\"", rec.Code, rec.Body.String())
	}
	if p.Backends()[0].IsAlive() {
		t.Error("dead backend should have been taken out of rotation")
	}
}

func TestDoesNotRetryPost(t *testing.T) {
	live, hits := startBackend(t, "live")
	p, err := New([]string{deadURL(), live.URL}, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}

	rec := do(t, p, http.MethodPost, strings.NewReader("order=1"))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("got %d, want 502", rec.Code)
	}
	if hits.Load() != 0 {
		t.Errorf("POST was retried: live backend got %d requests", hits.Load())
	}
}

func TestAllBackendsDownIs503(t *testing.T) {
	p, err := New([]string{deadURL(), deadURL()}, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	if rec := do(t, p, http.MethodGet, nil); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("got %d, want 503", rec.Code)
	}
}

// A client hanging up isn't the backend's fault.
func TestClientCancelKeepsBackendAlive(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	p, err := New([]string{srv.URL}, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/x", nil).WithContext(ctx)
	go func() {
		<-entered
		cancel()
	}()
	p.ServeHTTP(httptest.NewRecorder(), req)

	if !p.Backends()[0].IsAlive() {
		t.Error("backend was marked down because the client went away")
	}
}

func TestHealthCheckTakesBackendsOutAndBringsThemBack(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(false)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("health check hit %s", r.URL.Path)
		}
		if !healthy.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)

	p, err := New([]string{srv.URL}, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	b := p.Backends()[0]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.HealthCheck(ctx, "/healthz", 10*time.Millisecond, time.Second)

	waitFor(t, func() bool { return !b.IsAlive() }, "backend to go down")
	healthy.Store(true)
	waitFor(t, b.IsAlive, "backend to come back")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestNewRejectsUnknownStrategy(t *testing.T) {
	if _, err := New(urls, "random"); err == nil {
		t.Error("expected an error for an unknown strategy")
	}
}

func TestLeastConnectionsPicksTheLeastBusy(t *testing.T) {
	p, err := New(urls, LeastConnections)
	if err != nil {
		t.Fatal(err)
	}
	b := p.Backends()
	b[0].active.Store(5)
	b[1].active.Store(1)
	b[2].active.Store(3)

	for i := 0; i < 10; i++ {
		if got := p.Next(nil); got != b[1] {
			t.Fatalf("pick %d: got %s (in flight %d), want %s", i, got.URL, got.Active(), b[1].URL)
		}
	}
}

// When every backend is equally busy, least connections must share requests
// out in turn, not send them all to the first backend in the list.
func TestLeastConnectionsSharesTiesInTurn(t *testing.T) {
	p, err := New(urls, LeastConnections)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		counts[p.Next(nil).URL.Host]++
	}
	for _, u := range p.Backends() {
		if counts[u.URL.Host] != 100 {
			t.Errorf("idle backends shared unevenly: %v", counts)
			break
		}
	}
}

func TestLeastConnectionsSkipsDeadBackends(t *testing.T) {
	p, err := New(urls, LeastConnections)
	if err != nil {
		t.Fatal(err)
	}
	b := p.Backends()
	b[0].active.Store(9)
	b[2].active.Store(9)
	b[1].SetAlive(false, "test") // the least busy, but dead

	if got := p.Next(nil); got == b[1] {
		t.Error("picked a dead backend")
	}
}

// The in-flight count must go back to zero after each request, including
// one that failed and was retried elsewhere.
func TestInFlightCountReturnsToZero(t *testing.T) {
	live, _ := startBackend(t, "live")
	p, err := New([]string{deadURL(), live.URL}, LeastConnections)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		do(t, p, http.MethodGet, nil)
	}
	for _, b := range p.Backends() {
		if b.Active() != 0 {
			t.Errorf("%s still has %d in flight", b.URL, b.Active())
		}
	}
}

// While a request is being handled, its backend counts it as in flight, so
// least connections sends the next request elsewhere.
func TestInFlightRequestSteersTheNextOneAway(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	}))
	t.Cleanup(slow.Close)
	fast, _ := startBackend(t, "fast")

	p, err := New([]string{slow.URL, fast.URL}, LeastConnections)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		do(t, p, http.MethodGet, nil) // ties go in turn, so this one goes to slow
		close(done)
	}()
	<-entered

	if p.Backends()[0].Active() != 1 {
		t.Fatalf("slow backend in flight = %d, want 1", p.Backends()[0].Active())
	}
	for i := 0; i < 5; i++ {
		if got := p.Next(nil); got != p.Backends()[1] {
			t.Errorf("pick %d went to the busy backend", i)
		}
	}
	close(release)
	<-done
}

// With one backend busy, the idle ones must split its share evenly between
// them, not hand it all to whichever comes first in the list.
func TestLeastConnectionsSharesTiesFairlyAroundABusyBackend(t *testing.T) {
	p, err := New(urls, LeastConnections)
	if err != nil {
		t.Fatal(err)
	}
	busy := p.Backends()[2]
	busy.active.Store(4)

	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		counts[p.Next(nil).URL.Host]++
	}
	if counts[busy.URL.Host] != 0 || counts["localhost:8081"] != 150 || counts["localhost:8082"] != 150 {
		t.Errorf("uneven split around a busy backend: %v", counts)
	}
}
