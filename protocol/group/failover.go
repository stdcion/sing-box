package group

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/batch"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

func RegisterFailover(registry *outbound.Registry) {
	outbound.Register[option.FailoverOutboundOptions](registry, C.TypeFailover, NewFailover)
}

var _ adapter.OutboundGroup = (*Failover)(nil)

type Failover struct {
	outbound.Adapter
	ctx                          context.Context
	router                       adapter.Router
	outbound                     adapter.OutboundManager
	connection                   adapter.ConnectionManager
	logger                       log.ContextLogger
	tags                         []string
	link                         string
	interval                     time.Duration
	idleTimeout                  time.Duration
	recoveryThreshold            int
	failureThreshold             int
	lazyHealthCheck              bool
	group                        *FailoverGroup
	interruptExternalConnections bool
}

func NewFailover(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.FailoverOutboundOptions) (adapter.Outbound, error) {
	outbound := &Failover{
		Adapter:                      outbound.NewAdapter(C.TypeFailover, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		router:                       router,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		connection:                   service.FromContext[adapter.ConnectionManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		link:                         options.URL,
		interval:                     time.Duration(options.Interval),
		idleTimeout:                  time.Duration(options.IdleTimeout),
		recoveryThreshold:            options.RecoveryThreshold,
		failureThreshold:             options.FailureThreshold,
		lazyHealthCheck:              options.LazyHealthCheck == nil || *options.LazyHealthCheck,
		interruptExternalConnections: options.InterruptExistConnections,
	}
	if len(outbound.tags) == 0 {
		return nil, E.New("missing tags")
	}
	return outbound, nil
}

func (s *Failover) Start() error {
	outbounds := make([]adapter.Outbound, 0, len(s.tags))
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		outbounds = append(outbounds, detour)
	}
	group, err := NewFailoverGroup(s.ctx, s.outbound, s.logger, outbounds, s.link, s.interval, s.idleTimeout, s.recoveryThreshold, s.failureThreshold, s.lazyHealthCheck, s.interruptExternalConnections)
	if err != nil {
		return err
	}
	s.group = group
	return nil
}

func (s *Failover) PostStart() error {
	s.group.PostStart()
	return nil
}

func (s *Failover) Close() error {
	return common.Close(
		common.PtrOrNil(s.group),
	)
}

func (s *Failover) Now() string {
	if s.group.selectedOutboundTCP != nil {
		return s.group.selectedOutboundTCP.Tag()
	} else if s.group.selectedOutboundUDP != nil {
		return s.group.selectedOutboundUDP.Tag()
	}
	return ""
}

func (s *Failover) All() []string {
	return s.tags
}

func (s *Failover) URLTest(ctx context.Context) (map[string]uint16, error) {
	return s.group.URLTest(ctx)
}

func (s *Failover) CheckOutbounds() {
	s.group.CheckOutbounds(true)
}

func (s *Failover) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	s.group.Touch()
	var outbound adapter.Outbound
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		outbound = s.group.selectedOutboundTCP
	case N.NetworkUDP:
		outbound = s.group.selectedOutboundUDP
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if outbound == nil {
		outbound, _ = s.group.Select(network)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.DialContext(ctx, network, destination)
	if err == nil {
		return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.history.DeleteURLTestHistory(RealTag(outbound))
	return nil, err
}

func (s *Failover) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	s.group.Touch()
	outbound := s.group.selectedOutboundUDP
	if outbound == nil {
		outbound, _ = s.group.Select(N.NetworkUDP)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.ListenPacket(ctx, destination)
	if err == nil {
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.history.DeleteURLTestHistory(RealTag(outbound))
	return nil, err
}

func (s *Failover) NewConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewConnection(ctx, s, conn, metadata, onClose)
}

func (s *Failover) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewPacketConnection(ctx, s, conn, metadata, onClose)
}

func (s *Failover) NewDirectRouteConnection(metadata adapter.InboundContext, routeContext tun.DirectRouteContext, timeout time.Duration) (tun.DirectRouteDestination, error) {
	s.group.Touch()
	selected := s.group.selectedOutboundTCP
	if selected == nil {
		selected, _ = s.group.Select(N.NetworkTCP)
	}
	if selected == nil {
		return nil, E.New("missing supported outbound")
	}
	if !common.Contains(selected.Network(), metadata.Network) {
		return nil, E.New(metadata.Network, " is not supported by outbound: ", selected.Tag())
	}
	return selected.(adapter.DirectRouteOutbound).NewDirectRouteConnection(metadata, routeContext, timeout)
}

// FailoverGroup implements the failover selection logic with hysteresis.
type FailoverGroup struct {
	ctx                          context.Context
	router                       adapter.Router
	outbound                     adapter.OutboundManager
	pause                        pause.Manager
	pauseCallback                *list.Element[pause.Callback]
	logger                       log.Logger
	outbounds                    []adapter.Outbound
	link                         string
	interval                     time.Duration
	idleTimeout                  time.Duration
	recoveryThreshold            int
	failureThreshold             int
	lazyHealthCheck              bool
	history                      adapter.URLTestHistoryStorage
	checking                     atomic.Bool
	recoveryCounts               []atomic.Int32
	failureCounts                []atomic.Int32
	available                    []atomic.Bool
	selectedOutboundTCP          adapter.Outbound
	selectedOutboundUDP          adapter.Outbound
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	access                       sync.Mutex
	ticker                       *time.Ticker
	close                        chan struct{}
	started                      bool
	lastActive                   common.TypedValue[time.Time]
}

func NewFailoverGroup(
	ctx context.Context,
	outboundManager adapter.OutboundManager,
	logger log.Logger,
	outbounds []adapter.Outbound,
	link string,
	interval time.Duration,
	idleTimeout time.Duration,
	recoveryThreshold int,
	failureThreshold int,
	lazyHealthCheck bool,
	interruptExternalConnections bool,
) (*FailoverGroup, error) {
	if interval == 0 {
		interval = C.DefaultURLTestInterval
	}
	if idleTimeout == 0 {
		idleTimeout = C.DefaultURLTestIdleTimeout
	}
	if interval > idleTimeout {
		return nil, E.New("interval must be less or equal than idle_timeout")
	}
	if recoveryThreshold <= 0 {
		recoveryThreshold = 3
	}
	if failureThreshold <= 0 {
		failureThreshold = 1
	}
	var history adapter.URLTestHistoryStorage
	if historyFromCtx := service.PtrFromContext[urltest.HistoryStorage](ctx); historyFromCtx != nil {
		history = historyFromCtx
	} else if clashServer := service.FromContext[adapter.ClashServer](ctx); clashServer != nil {
		history = clashServer.HistoryStorage()
	} else {
		history = urltest.NewHistoryStorage()
	}
	recoveryCounts := make([]atomic.Int32, len(outbounds))
	failCounts := make([]atomic.Int32, len(outbounds))
	avail := make([]atomic.Bool, len(outbounds))
	for i := range avail {
		avail[i].Store(true)
	}
	return &FailoverGroup{
		ctx:                          ctx,
		outbound:                     outboundManager,
		logger:                       logger,
		outbounds:                    outbounds,
		link:                         link,
		interval:                     interval,
		idleTimeout:                  idleTimeout,
		recoveryThreshold:            recoveryThreshold,
		failureThreshold:             failureThreshold,
		lazyHealthCheck:              lazyHealthCheck,
		history:                      history,
		close:                        make(chan struct{}),
		pause:                        service.FromContext[pause.Manager](ctx),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: interruptExternalConnections,
		recoveryCounts:               recoveryCounts,
		failureCounts:                failCounts,
		available:                    avail,
	}, nil
}

func (g *FailoverGroup) PostStart() {
	g.access.Lock()
	defer g.access.Unlock()
	g.started = true
	g.lastActive.Store(time.Now())
	g.selectedOutboundTCP, _ = g.Select(N.NetworkTCP)
	g.selectedOutboundUDP, _ = g.Select(N.NetworkUDP)
	go g.CheckOutbounds(false)
}

func (g *FailoverGroup) Touch() {
	if !g.started {
		return
	}
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker != nil {
		g.lastActive.Store(time.Now())
		return
	}
	g.ticker = time.NewTicker(g.interval)
	go g.loopCheck()
	g.pauseCallback = pause.RegisterTicker(g.pause, g.ticker, g.interval, nil)
}

func (g *FailoverGroup) Close() error {
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker == nil {
		return nil
	}
	g.ticker.Stop()
	g.pause.UnregisterCallback(g.pauseCallback)
	close(g.close)
	return nil
}

// Select returns the highest-priority available outbound for the given network.
func (g *FailoverGroup) Select(network string) (adapter.Outbound, bool) {
	for i, detour := range g.outbounds {
		if !common.Contains(detour.Network(), network) {
			continue
		}
		if g.available[i].Load() {
			return detour, true
		}
	}
	// All unavailable — keep current selection
	return nil, false
}

func (g *FailoverGroup) loopCheck() {
	if time.Since(g.lastActive.Load()) > g.interval {
		g.lastActive.Store(time.Now())
		g.CheckOutbounds(false)
	}
	for {
		select {
		case <-g.close:
			return
		case <-g.ticker.C:
		}
		if time.Since(g.lastActive.Load()) > g.idleTimeout {
			g.access.Lock()
			g.ticker.Stop()
			g.ticker = nil
			g.pause.UnregisterCallback(g.pauseCallback)
			g.pauseCallback = nil
			g.access.Unlock()
			return
		}
		g.CheckOutbounds(false)
	}
}

func (g *FailoverGroup) CheckOutbounds(force bool) {
	_, _ = g.urlTest(g.ctx, force)
}

func (g *FailoverGroup) URLTest(ctx context.Context) (map[string]uint16, error) {
	return g.urlTest(ctx, true)
}

func (g *FailoverGroup) urlTest(ctx context.Context, force bool) (map[string]uint16, error) {
	result := make(map[string]uint16)
	if g.checking.Swap(true) {
		return result, nil
	}
	defer g.checking.Store(false)
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	checked := make(map[string]bool)
	var resultAccess sync.Mutex
	maxIndex := len(g.outbounds) - 1
	if g.lazyHealthCheck && !force {
		maxIndex = 0
		for i, detour := range g.outbounds {
			if detour == g.selectedOutboundTCP || detour == g.selectedOutboundUDP {
				if i > maxIndex {
					maxIndex = i
				}
			}
		}
		if !g.available[maxIndex].Load() {
			maxIndex = len(g.outbounds) - 1
		}
	}
	for i, detour := range g.outbounds {
		if i > maxIndex {
			break
		}
		tag := detour.Tag()
		realTag := RealTag(detour)
		if checked[realTag] {
			continue
		}
		checked[realTag] = true
		p, loaded := g.outbound.Outbound(realTag)
		if !loaded {
			continue
		}
		idx := i
		b.Go(realTag, func() (any, error) {
			testCtx, cancel := context.WithTimeout(g.ctx, C.TCPTimeout)
			defer cancel()
			t, err := urltest.URLTest(testCtx, g.link, p)
			if err != nil {
				g.history.DeleteURLTestHistory(realTag)
				g.recoveryCounts[idx].Store(0)
				if g.available[idx].Load() {
					count := g.failureCounts[idx].Add(1)
					if int(count) >= g.failureThreshold {
						g.failureCounts[idx].Store(0)
						g.available[idx].Store(false)
						g.logger.Info("outbound ", tag, " unavailable after ", count, "/", g.failureThreshold, " failures: ", err)
					} else {
						g.logger.Info("outbound ", tag, " failure ", count, "/", g.failureThreshold, ": ", err)
					}
				} else {
					g.failureCounts[idx].Store(0)
					g.logger.Debug("outbound ", tag, " still unavailable: ", err)
				}
			} else {
				g.history.StoreURLTestHistory(realTag, &adapter.URLTestHistory{
					Time:  time.Now(),
					Delay: t,
				})
				g.failureCounts[idx].Store(0)
				if !g.available[idx].Load() {
					count := g.recoveryCounts[idx].Add(1)
					if int(count) >= g.recoveryThreshold {
						g.recoveryCounts[idx].Store(0)
						g.available[idx].Store(true)
						g.logger.Info("outbound ", tag, " recovered after ", count, "/", g.recoveryThreshold, " tests")
					} else {
						g.logger.Info("outbound ", tag, " recovery ", count, "/", g.recoveryThreshold)
					}
				} else {
					g.logger.Debug("outbound ", tag, " healthy: ", t, "ms")
				}
				resultAccess.Lock()
				result[tag] = t
				resultAccess.Unlock()
			}
			return nil, nil
		})
	}
	b.Wait()
	g.performUpdateCheck()
	return result, nil
}

func (g *FailoverGroup) performUpdateCheck() {
	g.access.Lock()
	defer g.access.Unlock()
	var updated bool
	if outbound, _ := g.Select(N.NetworkTCP); outbound != nil && outbound != g.selectedOutboundTCP {
		if g.selectedOutboundTCP != nil {
			updated = true
			g.logger.Info(g.selectedOutboundTCP.Tag(), " -> ", outbound.Tag())
		}
		g.selectedOutboundTCP = outbound
	}
	if outbound, _ := g.Select(N.NetworkUDP); outbound != nil && outbound != g.selectedOutboundUDP {
		if g.selectedOutboundUDP != nil {
			updated = true
			if g.selectedOutboundTCP != outbound {
				g.logger.Info(g.selectedOutboundUDP.Tag(), " -> ", outbound.Tag(), " UDP")
			}
		}
		g.selectedOutboundUDP = outbound
	}
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
}
