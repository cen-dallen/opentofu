// Copyright (c) The OpenTofu Authors
// SPDX-License-Identifier: MPL-2.0
// Copyright (c) 2023 HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package plugins

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/opentofu/opentofu/internal/addrs"
	"github.com/opentofu/opentofu/internal/providers"
	"github.com/opentofu/opentofu/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
)

// ProviderInstanceLimitEnvVar caps the number of configured provider plugin
// processes that a single provider manager keeps alive at once. Unset or 0
// means unlimited, which is the upstream behaviour.
const ProviderInstanceLimitEnvVar = "TOFU_PROVIDER_INSTANCE_LIMIT"

func providerInstanceLimitFromEnv() int {
	raw := os.Getenv(ProviderInstanceLimitEnvVar)
	if raw == "" {
		return 0
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		log.Printf("[WARN] ignoring invalid %s=%q: must be a non-negative integer", ProviderInstanceLimitEnvVar, raw)
		return 0
	}
	return limit
}

// ProviderLazyStartEnvVar, when true and the pool is enabled, defers starting
// and configuring each provider instance until its first RPC instead of doing
// it where the provider is configured. Starts then run concurrently with the
// graph walk rather than serially in the provider node, and instances that are
// never used are never started. Configuration and credential errors are
// reported on the first resource that uses the instance.
const ProviderLazyStartEnvVar = "TOFU_PROVIDER_LAZY_START"

func providerLazyStartFromEnv() bool {
	raw := os.Getenv(ProviderLazyStartEnvVar)
	if raw == "" {
		return false
	}
	lazy, err := strconv.ParseBool(raw)
	if err != nil {
		log.Printf("[WARN] ignoring invalid %s=%q: must be a boolean", ProviderLazyStartEnvVar, raw)
		return false
	}
	return lazy
}

// providerPool bounds the number of live configured provider processes.
//
// Each configured provider instance is represented by a pooledProvider proxy
// which remembers its configuration. When an RPC arrives for an instance whose
// process is not running, the pool waits for a free slot (closing the least
// recently used idle process if needed) and restarts and reconfigures the
// instance before forwarding the call.
//
// Invariants:
//   - at most one live process per pooledProvider;
//   - only processes with no in-flight RPCs and no open ephemeral resources are evicted;
//   - an RPC never waits on another provider instance, so busy instances always
//     become idle eventually. The only state where waiting cannot make progress is
//     when every live process is pinned by an open ephemeral resource; the pool then
//     exceeds the limit rather than deadlock.
type providerPool struct {
	limit int
	lazy  bool

	mu sync.Mutex
	// changed is closed and replaced whenever a slot may have become available.
	changed chan struct{}
	// live counts running (or starting) processes.
	live int
	// busy counts instances that are starting or have in-flight RPCs.
	busy int
	// idle holds live, unpinned instances with no in-flight RPCs, oldest first.
	idle    *list.List
	stopped bool

	starts   int
	restarts int
}

func newProviderPool(limit int, lazy bool) *providerPool {
	return &providerPool{
		limit:   limit,
		lazy:    lazy,
		changed: make(chan struct{}),
		idle:    list.New(),
	}
}

// notifyLocked wakes all waiters. Must be called with mu held.
func (pp *providerPool) notifyLocked() {
	close(pp.changed)
	pp.changed = make(chan struct{})
}

type startFunc func(ctx context.Context) (providers.Interface, tfdiags.Diagnostics)

// pooledProvider is a providers.Configured whose underlying plugin process may be
// stopped and restarted transparently by its providerPool.
type pooledProvider struct {
	pool    *providerPool
	manager *providerManager
	addr    addrs.Provider
	start   startFunc

	// The fields below are guarded by pool.mu.
	inst     providers.Interface
	starting bool
	inflight int
	pins     int
	closed   bool
	stopped  bool
	everLive bool
	idleElem *list.Element
}

var _ providers.Configured = (*pooledProvider)(nil)

// acquire returns a live process for this instance, starting it if necessary,
// and registers an in-flight RPC that must be ended with release.
func (p *pooledProvider) acquire(ctx context.Context) (providers.Interface, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	pp := p.pool
	var victim providers.Interface

	pp.mu.Lock()
	for {
		if p.closed {
			pp.mu.Unlock()
			return nil, diags.Append(fmt.Errorf("bug: provider %s used after it was closed", p.addr))
		}
		if p.inst != nil && !p.starting {
			if p.inflight == 0 {
				p.removeIdleLocked()
				pp.busy++
			}
			p.inflight++
			inst := p.inst
			pp.mu.Unlock()
			return inst, diags
		}
		if p.stopped || pp.stopped {
			pp.mu.Unlock()
			return nil, diags.Append(fmt.Errorf("provider %s was stopped and cannot be restarted", p.addr))
		}
		if !p.starting {
			if pp.live < pp.limit {
				pp.live++
				break
			}
			if front := pp.idle.Front(); front != nil {
				// Hand the evicted process's slot straight to this instance.
				evicted := front.Value.(*pooledProvider)
				evicted.removeIdleLocked()
				victim = evicted.inst
				evicted.inst = nil
				break
			}
			if pp.busy == 0 {
				pp.live++
				log.Printf("[WARN] provider instance limit %d exceeded (%d live): every live provider instance has open ephemeral resources", pp.limit, pp.live)
				break
			}
		}
		ch := pp.changed
		pp.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, diags.Append(fmt.Errorf("waiting to start provider %s: %w", p.addr, ctx.Err()))
		}
		pp.mu.Lock()
	}
	p.starting = true
	pp.busy++
	isRestart := p.everLive
	pp.mu.Unlock()

	if victim != nil {
		if err := victim.Close(context.WithoutCancel(ctx)); err != nil {
			log.Printf("[WARN] error closing evicted provider instance: %s", err)
		}
	}

	if isRestart {
		log.Printf("[TRACE] plugins.providerPool: restarting provider %s", p.addr)
	}
	inst, startDiags := p.start(ctx)

	pp.mu.Lock()
	p.starting = false
	if isRestart {
		pp.restarts++
	} else {
		pp.starts++
	}
	if startDiags.HasErrors() || p.closed || p.stopped || pp.stopped {
		pp.live--
		pp.busy--
		pp.notifyLocked()
		pp.mu.Unlock()
		if inst != nil {
			_ = inst.Close(context.WithoutCancel(ctx))
		}
		if startDiags.HasErrors() && isRestart {
			return nil, diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Failed to restart provider",
				fmt.Sprintf("The provider %s was stopped to stay within %s=%d and failed to restart:\n\n%s", p.addr, ProviderInstanceLimitEnvVar, pp.limit, startDiags.ErrWithWarnings()),
			))
		}
		if startDiags.HasErrors() && pp.lazy {
			return nil, diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Failed to configure provider",
				fmt.Sprintf("The provider %s failed to configure when first used (%s is set, so provider configuration is deferred until first use):\n\n%s", p.addr, ProviderLazyStartEnvVar, startDiags.ErrWithWarnings()),
			))
		}
		if startDiags.HasErrors() {
			return nil, startDiags
		}
		return nil, diags.Append(fmt.Errorf("provider %s was closed or stopped while starting", p.addr))
	}
	p.inst = inst
	p.everLive = true
	p.inflight++
	pp.notifyLocked()
	pp.mu.Unlock()
	return inst, startDiags
}

// release ends an in-flight RPC. pinDelta adjusts the number of open ephemeral
// resources, which keep the process from being evicted.
func (p *pooledProvider) release(pinDelta int) {
	pp := p.pool
	pp.mu.Lock()
	defer pp.mu.Unlock()
	p.pins += pinDelta
	p.inflight--
	if p.inflight > 0 {
		return
	}
	pp.busy--
	if p.inst != nil && p.pins <= 0 {
		p.idleElem = pp.idle.PushBack(p)
	}
	pp.notifyLocked()
}

func (p *pooledProvider) removeIdleLocked() {
	if p.idleElem != nil {
		p.pool.idle.Remove(p.idleElem)
		p.idleElem = nil
	}
}

func (p *pooledProvider) Close(ctx context.Context) error {
	pp := p.pool
	pp.mu.Lock()
	if p.closed {
		pp.mu.Unlock()
		return nil
	}
	p.closed = true
	inst := p.inst
	p.inst = nil
	if inst != nil {
		p.removeIdleLocked()
		pp.live--
		pp.notifyLocked()
		if pp.live == 0 {
			pp.logStatsLocked()
		}
	}
	pp.mu.Unlock()
	if inst == nil {
		return nil
	}
	return inst.Close(ctx)
}

func (p *pooledProvider) Stop(ctx context.Context) error {
	pp := p.pool
	pp.mu.Lock()
	p.stopped = true
	inst := p.inst
	pp.notifyLocked()
	pp.mu.Unlock()
	if inst == nil {
		return nil
	}
	return inst.Stop(ctx)
}

// stopAll prevents any further process starts in this pool.
func (pp *providerPool) stopAll() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.stopped = true
	pp.notifyLocked()
}

func (pp *providerPool) logStats() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.logStatsLocked()
}

func (pp *providerPool) logStatsLocked() {
	if pp.starts > 0 {
		log.Printf("[INFO] plugins.providerPool: limit=%d lazy=%t starts=%d restarts=%d", pp.limit, pp.lazy, pp.starts, pp.restarts)
	}
}

// GetProviderSchema is served from the manager's schema cache so that it never
// needs to restart a process.
func (p *pooledProvider) GetProviderSchema(ctx context.Context) providers.GetProviderSchemaResponse {
	schema, diags := p.manager.GetProviderSchema(ctx, p.addr)
	schema.Diagnostics = diags
	return schema
}

func (p *pooledProvider) ValidateProviderConfig(ctx context.Context, req providers.ValidateProviderConfigRequest) providers.ValidateProviderConfigResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ValidateProviderConfigResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ValidateProviderConfig(ctx, req)
}

func (p *pooledProvider) ValidateResourceConfig(ctx context.Context, req providers.ValidateResourceConfigRequest) providers.ValidateResourceConfigResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ValidateResourceConfigResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ValidateResourceConfig(ctx, req)
}

func (p *pooledProvider) ValidateDataResourceConfig(ctx context.Context, req providers.ValidateDataResourceConfigRequest) providers.ValidateDataResourceConfigResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ValidateDataResourceConfigResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ValidateDataResourceConfig(ctx, req)
}

func (p *pooledProvider) ValidateEphemeralConfig(ctx context.Context, req providers.ValidateEphemeralConfigRequest) providers.ValidateEphemeralConfigResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ValidateEphemeralConfigResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ValidateEphemeralConfig(ctx, req)
}

func (p *pooledProvider) MoveResourceState(ctx context.Context, req providers.MoveResourceStateRequest) providers.MoveResourceStateResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.MoveResourceStateResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.MoveResourceState(ctx, req)
}

func (p *pooledProvider) CallFunction(ctx context.Context, req providers.CallFunctionRequest) providers.CallFunctionResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.CallFunctionResponse{Error: diags.Err()}
	}
	defer p.release(0)
	return inst.CallFunction(ctx, req)
}

// ConfigureProvider is not expected to be called on an already configured
// instance; it is forwarded unchanged and does not affect restarts.
func (p *pooledProvider) ConfigureProvider(ctx context.Context, req providers.ConfigureProviderRequest) providers.ConfigureProviderResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ConfigureProviderResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ConfigureProvider(ctx, req)
}

func (p *pooledProvider) UpgradeResourceState(ctx context.Context, req providers.UpgradeResourceStateRequest) providers.UpgradeResourceStateResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.UpgradeResourceStateResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.UpgradeResourceState(ctx, req)
}

func (p *pooledProvider) UpgradeResourceIdentity(ctx context.Context, req providers.UpgradeResourceIdentityRequest) providers.UpgradeResourceIdentityResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.UpgradeResourceIdentityResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.UpgradeResourceIdentity(ctx, req)
}

func (p *pooledProvider) ReadResource(ctx context.Context, req providers.ReadResourceRequest) providers.ReadResourceResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ReadResourceResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ReadResource(ctx, req)
}

func (p *pooledProvider) PlanResourceChange(ctx context.Context, req providers.PlanResourceChangeRequest) providers.PlanResourceChangeResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.PlanResourceChangeResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.PlanResourceChange(ctx, req)
}

func (p *pooledProvider) ApplyResourceChange(ctx context.Context, req providers.ApplyResourceChangeRequest) providers.ApplyResourceChangeResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ApplyResourceChangeResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ApplyResourceChange(ctx, req)
}

func (p *pooledProvider) ImportResourceState(ctx context.Context, req providers.ImportResourceStateRequest) providers.ImportResourceStateResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ImportResourceStateResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ImportResourceState(ctx, req)
}

func (p *pooledProvider) ReadDataSource(ctx context.Context, req providers.ReadDataSourceRequest) providers.ReadDataSourceResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.ReadDataSourceResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.ReadDataSource(ctx, req)
}

func (p *pooledProvider) OpenEphemeralResource(ctx context.Context, req providers.OpenEphemeralResourceRequest) providers.OpenEphemeralResourceResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.OpenEphemeralResourceResponse{Diagnostics: diags}
	}
	resp := inst.OpenEphemeralResource(ctx, req)
	pin := 0
	if !resp.Diagnostics.HasErrors() {
		pin = 1
	}
	p.release(pin)
	return resp
}

func (p *pooledProvider) RenewEphemeralResource(ctx context.Context, req providers.RenewEphemeralResourceRequest) providers.RenewEphemeralResourceResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.RenewEphemeralResourceResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.RenewEphemeralResource(ctx, req)
}

func (p *pooledProvider) CloseEphemeralResource(ctx context.Context, req providers.CloseEphemeralResourceRequest) providers.CloseEphemeralResourceResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.CloseEphemeralResourceResponse{Diagnostics: diags}
	}
	resp := inst.CloseEphemeralResource(ctx, req)
	// Unpin even if the close failed: the resource will not be retried.
	p.release(-1)
	return resp
}

func (p *pooledProvider) GetFunctions(ctx context.Context) providers.GetFunctionsResponse {
	inst, diags := p.acquire(ctx)
	if diags.HasErrors() {
		return providers.GetFunctionsResponse{Diagnostics: diags}
	}
	defer p.release(0)
	return inst.GetFunctions(ctx)
}

// newPooledConfiguredProvider creates a pooled instance. Unless the pool is
// lazy, it starts the instance once eagerly so that configuration errors
// surface where the provider is configured.
func (p *providerManager) newPooledConfiguredProvider(ctx context.Context, addr addrs.Provider, unmarkedConfigVal cty.Value) (providers.Configured, tfdiags.Diagnostics) {
	proxy := &pooledProvider{
		pool:    p.pool,
		manager: p,
		addr:    addr,
		start: func(ctx context.Context) (providers.Interface, tfdiags.Diagnostics) {
			return p.startConfiguredProvider(ctx, addr, unmarkedConfigVal)
		},
	}

	p.instancesLock.Lock()
	if p.isShutdown.Load() {
		p.instancesLock.Unlock()
		return nil, tfdiags.Diagnostics{}.Append(fmt.Errorf("bug: unable to start provider %s, manager is shutdown", addr))
	}
	p.instances = append(p.instances, proxy)
	p.instancesLock.Unlock()

	if p.pool.lazy {
		return proxy, nil
	}

	_, diags := proxy.acquire(ctx)
	if !diags.HasErrors() {
		proxy.release(0)
	}
	return proxy, diags
}
