package pool

import (
	"sync"
	"testing"
)

var urls = []string{
	"http://localhost:8081",
	"http://localhost:8082",
	"http://localhost:8083",
}

func TestNextRotatesInOrder(t *testing.T) {
	p, err := New(urls)
	if err != nil {
		t.Fatal(err)
	}

	// Two full laps: 8081, 8082, 8083, 8081, 8082, 8083.
	for i := 0; i < 6; i++ {
		want := urls[i%len(urls)]
		if got := p.Next().URL.String(); got != want {
			t.Errorf("pick %d: got %s, want %s", i, got, want)
		}
	}
}

// Many goroutines picking at once must still spread picks perfectly evenly.
// Run with `go test -race` to also check for data races.
func TestNextIsEvenUnderConcurrency(t *testing.T) {
	p, err := New(urls)
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
				local[p.Next().URL.Host]++
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
		if _, err := New(in); err == nil {
			t.Errorf("%s: expected an error for %v", name, in)
		}
	}
}
