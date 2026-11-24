// Copyright (c) The OpenTofu Authors
// SPDX-License-Identifier: MPL-2.0
// Copyright (c) 2023 HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package plugins

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opentofu/opentofu/internal/addrs"
	"github.com/opentofu/opentofu/internal/providers"
	"github.com/zclconf/go-cty/cty"
)

var poolTestAddr = addrs.NewDefaultProvider("pooltest")

// poolTracker records how many fake provider processes are alive.
type poolTracker struct {
	mu        sync.Mutex
	live      int
	maxLive   int
	liveByID  map[string]int
	starts    map[string]int
	failStart func(id string, n int) bool
	block     chan struct{}
}

func newPoolTracker() *poolTracker {
	return &poolTracker{liveByID: map[string]int{}, starts: map[string]int{}}
}

func (tr *poolTracker) manager(t *testing.T, limit string) ProviderManager {
	t.Helper()
	t.Setenv(ProviderInstanceLimitEnvVar, limit)
	lib := NewLibrary(ProviderFactories{
		poolTestAddr: func() (providers.Interface, error) { return &fakePooledProvider{tr: tr}, nil },
	}, nil)
	return lib.NewProviderManager()
}

func (tr *poolTracker) snapshot() (live, maxLive int) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.live, tr.maxLive
}

func (tr *poolTracker) startsOf(id string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.starts[id]
}

// fakePooledProvider is one fake plugin process. Unimplemented methods panic
// via the nil embedded interface.
type fakePooledProvider struct {
	providers.Interface
	tr     *poolTracker
	id     string
	closed bool
}

func (f *fakePooledProvider) ValidateProviderConfig(_ context.Context, _ providers.ValidateProviderConfigRequest) providers.ValidateProviderConfigResponse {
	return providers.ValidateProviderConfigResponse{}
}

func (f *fakePooledProvider) ConfigureProvider(_ context.Context, req providers.ConfigureProviderRequest) (resp providers.ConfigureProviderResponse) {
	id := req.Config.GetAttr("id").AsString()
	tr := f.tr
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.starts[id]++
	if tr.failStart != nil && tr.failStart(id, tr.starts[id]) {
		resp.Diagnostics = resp.Diagnostics.Append(errors.New("sts throttled"))
		return resp
	}
	f.id = id
	tr.live++
	tr.liveByID[id]++
	if tr.live > tr.maxLive {
		tr.maxLive = tr.live
	}
	if tr.liveByID[id] > 1 {
		panic(fmt.Sprintf("two live processes for instance %s", id))
	}
	return resp
}

func (f *fakePooledProvider) use() {
	f.tr.mu.Lock()
	defer f.tr.mu.Unlock()
	if f.closed || f.id == "" {
		panic("RPC on a closed or unconfigured fake provider")
	}
}

func (f *fakePooledProvider) ReadResource(_ context.Context, _ providers.ReadResourceRequest) providers.ReadResourceResponse {
	f.use()
	if b := f.tr.block; b != nil {
		<-b
	} else {
		time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond)
	}
	return providers.ReadResourceResponse{NewState: cty.StringVal(f.id)}
}

func (f *fakePooledProvider) OpenEphemeralResource(_ context.Context, _ providers.OpenEphemeralResourceRequest) providers.OpenEphemeralResourceResponse {
	f.use()
	return providers.OpenEphemeralResourceResponse{}
}

func (f *fakePooledProvider) CloseEphemeralResource(_ context.Context, _ providers.CloseEphemeralResourceRequest) providers.CloseEphemeralResourceResponse {
	f.use()
	return providers.CloseEphemeralResourceResponse{}
}

func (f *fakePooledProvider) Stop(context.Context) error { return nil }

func (f *fakePooledProvider) Close(context.Context) error {
	tr := f.tr
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if f.closed || f.id == "" {
		f.closed = true
		return nil
	}
	f.closed = true
	tr.live--
	tr.liveByID[f.id]--
	return nil
}

func poolCfg(id string) cty.Value {
	return cty.ObjectVal(map[string]cty.Value{"id": cty.StringVal(id)})
}

func newPooled(t *testing.T, m ProviderManager, id string) providers.Configured {
	t.Helper()
	p, diags := m.NewConfiguredProvider(t.Context(), poolTestAddr, poolCfg(id))
	if diags.HasErrors() {
		t.Fatalf("configuring %s: %s", id, diags.Err())
	}
	return p
}

func readID(t *testing.T, p providers.Configured) string {
	t.Helper()
	resp := p.ReadResource(t.Context(), providers.ReadResourceRequest{})
	if resp.Diagnostics.HasErrors() {
		t.Fatalf("ReadResource: %s", resp.Diagnostics.Err())
	}
	return resp.NewState.AsString()
}

func TestProviderPool_disabledByDefault(t *testing.T) {
	t.Setenv(ProviderLazyStartEnvVar, "true") // has no effect without a limit
	for _, v := range []string{"", "0", "bogus", "-3"} {
		tr := newPoolTracker()
		m := tr.manager(t, v)
		if _, ok := newPooled(t, m, "a").(*pooledProvider); ok {
			t.Errorf("%s=%q: got pooled provider, want unpooled", ProviderInstanceLimitEnvVar, v)
		}
	}
}

func TestProviderPool_evictsAndRestartsAtLimitOne(t *testing.T) {
	tr := newPoolTracker()
	m := tr.manager(t, "1")
	a := newPooled(t, m, "a")
	b := newPooled(t, m, "b") // evicts a

	if got := readID(t, b); got != "b" {
		t.Fatalf("b served by %q", got)
	}
	if got := readID(t, a); got != "a" { // restarts a, evicts b
		t.Fatalf("a served by %q", got)
	}
	if got := readID(t, a); got != "a" { // a still live, no restart
		t.Fatalf("a served by %q", got)
	}
	if live, maxLive := tr.snapshot(); live != 1 || maxLive != 1 {
		t.Fatalf("live=%d maxLive=%d, want 1/1", live, maxLive)
	}
	if got := tr.startsOf("a"); got != 2 {
		t.Fatalf("a started %d times, want 2", got)
	}
	if err := m.CloseAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if live, _ := tr.snapshot(); live != 0 {
		t.Fatalf("%d processes left after CloseAll", live)
	}
}

func TestProviderPool_concurrentNeverExceedsLimit(t *testing.T) {
	const limit, instances, workers, calls = 3, 25, 40, 50
	tr := newPoolTracker()
	m := tr.manager(t, fmt.Sprint(limit))
	ps := make([]providers.Configured, instances)
	for i := range ps {
		ps[i] = newPooled(t, m, fmt.Sprintf("i%d", i))
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := 0; c < calls; c++ {
				i := rand.IntN(instances)
				resp := ps[i].ReadResource(t.Context(), providers.ReadResourceRequest{})
				if resp.Diagnostics.HasErrors() {
					t.Errorf("ReadResource: %s", resp.Diagnostics.Err())
					return
				}
				if got, want := resp.NewState.AsString(), fmt.Sprintf("i%d", i); got != want {
					t.Errorf("instance %s served by %s", want, got)
				}
			}
		}()
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: workers did not finish")
	}
	if _, maxLive := tr.snapshot(); maxLive > limit {
		t.Fatalf("maxLive=%d exceeds limit %d", maxLive, limit)
	}
	_ = m.CloseAll(t.Context())
	if live, _ := tr.snapshot(); live != 0 {
		t.Fatalf("%d processes left after CloseAll", live)
	}
}

func TestProviderPool_ephemeralPinsAndOverflows(t *testing.T) {
	tr := newPoolTracker()
	m := tr.manager(t, "1")
	a := newPooled(t, m, "a")
	if resp := a.OpenEphemeralResource(t.Context(), providers.OpenEphemeralResourceRequest{}); resp.Diagnostics.HasErrors() {
		t.Fatal(resp.Diagnostics.Err())
	}

	// a is pinned and nothing is busy, so b must overflow rather than wait forever.
	b := newPooled(t, m, "b")
	readID(t, b)
	if live, _ := tr.snapshot(); live != 2 {
		t.Fatalf("live=%d, want 2 (pinned a + overflow b)", live)
	}
	if got := tr.startsOf("a"); got != 1 {
		t.Fatalf("pinned instance a was restarted (%d starts)", got)
	}

	if resp := a.CloseEphemeralResource(t.Context(), providers.CloseEphemeralResourceRequest{}); resp.Diagnostics.HasErrors() {
		t.Fatal(resp.Diagnostics.Err())
	}
	// Both are idle and over the limit; a new instance reuses a freed slot.
	c := newPooled(t, m, "c")
	readID(t, c)
	if live, _ := tr.snapshot(); live > 2 {
		t.Fatalf("live=%d after unpinning, want <= 2", live)
	}
}

func TestProviderPool_restartFailureIsReported(t *testing.T) {
	tr := newPoolTracker()
	tr.failStart = func(id string, n int) bool { return id == "a" && n == 2 }
	m := tr.manager(t, "1")
	a := newPooled(t, m, "a")
	newPooled(t, m, "b")

	resp := a.ReadResource(t.Context(), providers.ReadResourceRequest{})
	if !resp.Diagnostics.HasErrors() {
		t.Fatal("expected restart error")
	}
	if msg := resp.Diagnostics.Err().Error(); !strings.Contains(msg, "failed to restart") || !strings.Contains(msg, "sts throttled") {
		t.Fatalf("unexpected error: %s", msg)
	}
	// A later attempt retries and succeeds.
	if got := readID(t, a); got != "a" {
		t.Fatalf("a served by %q", got)
	}
}

func TestProviderPool_noRestartAfterStopAll(t *testing.T) {
	tr := newPoolTracker()
	m := tr.manager(t, "1")
	a := newPooled(t, m, "a")
	newPooled(t, m, "b")
	if err := m.StopAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	resp := a.ReadResource(t.Context(), providers.ReadResourceRequest{})
	if !resp.Diagnostics.HasErrors() {
		t.Fatal("expected error for restart after StopAll")
	}
	if got := tr.startsOf("a"); got != 1 {
		t.Fatalf("a restarted after StopAll (%d starts)", got)
	}
}

func TestProviderPool_waitHonoursContext(t *testing.T) {
	tr := newPoolTracker()
	tr.block = make(chan struct{})
	m := tr.manager(t, "1")
	a := newPooled(t, m, "a")
	b := newPooled(t, m, "b")

	started := make(chan struct{})
	go func() {
		close(started)
		b.ReadResource(context.Background(), providers.ReadResourceRequest{})
	}()
	<-started
	time.Sleep(20 * time.Millisecond) // let b's RPC take the only slot

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	resp := a.ReadResource(ctx, providers.ReadResourceRequest{})
	close(tr.block)
	if !resp.Diagnostics.HasErrors() || !strings.Contains(resp.Diagnostics.Err().Error(), "deadline") {
		t.Fatalf("expected context deadline error, got %v", resp.Diagnostics.Err())
	}
}

func TestProviderPool_lazyStart(t *testing.T) {
	tr := newPoolTracker()
	t.Setenv(ProviderLazyStartEnvVar, "true")
	m := tr.manager(t, "2")
	a := newPooled(t, m, "a")
	newPooled(t, m, "unused")
	if live, _ := tr.snapshot(); live != 0 {
		t.Fatalf("live=%d after configuring lazily, want 0", live)
	}
	if got := readID(t, a); got != "a" {
		t.Fatalf("a served by %q", got)
	}
	if tr.startsOf("a") != 1 || tr.startsOf("unused") != 0 {
		t.Fatalf("starts a=%d unused=%d, want 1/0", tr.startsOf("a"), tr.startsOf("unused"))
	}
}

func TestProviderPool_lazyStartConfigureErrorOnFirstUse(t *testing.T) {
	tr := newPoolTracker()
	tr.failStart = func(id string, _ int) bool { return id == "bad" }
	t.Setenv(ProviderLazyStartEnvVar, "1")
	m := tr.manager(t, "1")
	bad, diags := m.NewConfiguredProvider(t.Context(), poolTestAddr, poolCfg("bad"))
	if diags.HasErrors() {
		t.Fatalf("lazy configure reported errors before first use: %s", diags.Err())
	}
	resp := bad.ReadResource(t.Context(), providers.ReadResourceRequest{})
	if !resp.Diagnostics.HasErrors() {
		t.Fatal("expected configure error on first use")
	}
	if msg := resp.Diagnostics.Err().Error(); !strings.Contains(msg, ProviderLazyStartEnvVar) || !strings.Contains(msg, "sts throttled") {
		t.Fatalf("unexpected error: %s", msg)
	}
}
