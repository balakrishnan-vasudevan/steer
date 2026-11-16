package pool

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// HealthCheck polls every backend at path until ctx is cancelled, moving
// backends in and out of rotation. It blocks, so run it in a goroutine.
func (p *Pool) HealthCheck(ctx context.Context, path string, interval, timeout time.Duration) {
	client := &http.Client{
		Timeout: timeout,
		// A fresh connection every time, so a successful check proves a new
		// client could actually connect right now.
		Transport: &http.Transport{DisableKeepAlives: true},
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Check straight away, so a backend that's already dead at startup
	// doesn't have to wait a full interval to be noticed.
	p.checkAll(ctx, client, path)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.checkAll(ctx, client, path)
		}
	}
}

// checkAll checks every backend at the same time, so one hanging backend
// can't delay finding out about the others.
func (p *Pool) checkAll(ctx context.Context, client *http.Client, path string) {
	var wg sync.WaitGroup
	for _, b := range p.backends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := check(ctx, client, b, path); err != nil {
				b.SetAlive(false, "health check failed: "+err.Error())
				return
			}
			b.SetAlive(true, "health check passed")
		}()
	}
	wg.Wait()
}

func check(ctx context.Context, client *http.Client, b *Backend, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.URL.JoinPath(path).String(), nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
