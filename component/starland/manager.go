package starland

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/log"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

const (
	moduleName        = "starland.dataplane.com"
	authorizeMethod   = "AuthorizeUser"
	reportUsageMethod = "ReportUsage"
	nodeOnlineMethod  = "NodeOnline"
	heartbeatMethod   = "Heartbeat"
	nodeOfflineMethod = "NodeOffline"
	downloadBurstSize = 64 * 1024
)

type Config struct {
	Enabled                  bool   `yaml:"enabled" json:"enabled"`
	GatewayURL               string `yaml:"gateway-url" json:"gateway-url"`
	NodeID                   string `yaml:"node-id" json:"node-id"`
	NodePassword             string `yaml:"node-password" json:"node-password"`
	TimeoutSeconds           int    `yaml:"timeout-seconds" json:"timeout-seconds"`
	DenyTTLSeconds           int    `yaml:"deny-ttl-seconds" json:"deny-ttl-seconds"`
	ReportInterval           int    `yaml:"report-interval-seconds" json:"report-interval-seconds"`
	HeartbeatInterval        int    `yaml:"heartbeat-interval-seconds" json:"heartbeat-interval-seconds"`
	BatchSize                int    `yaml:"batch-size" json:"batch-size"`
	DefaultDownloadLimitMbps int    `yaml:"default-download-limit-mbps" json:"default-download-limit-mbps"`
}

type runtimeConfig struct {
	Config
	httpClient *http.Client
}

type userState struct {
	uplink            atomic.Uint64
	downlink          atomic.Uint64
	confirmedUplink   atomic.Uint64
	confirmedDownlink atomic.Uint64
	limiterMu         sync.RWMutex
	downloadLimiter   *rate.Limiter
	downloadLimitMbps uint32
}

type Manager struct {
	mu          sync.RWMutex
	config      *runtimeConfig
	denyUntil   map[string]time.Time
	connections map[string]map[uint64]io.Closer
	users       map[string]*userState
	nextConnID  uint64
	sequence    uint64
	bootID      string
	cancel      context.CancelFunc
	authGroup   singleflight.Group
	nodeDenied  bool
}

var Default = newManager()

func newManager() *Manager {
	boot := make([]byte, 16)
	if _, err := rand.Read(boot); err != nil {
		panic(fmt.Sprintf("generate Starland data-plane boot id: %v", err))
	}
	return &Manager{
		bootID:      hex.EncodeToString(boot),
		denyUntil:   make(map[string]time.Time),
		connections: make(map[string]map[uint64]io.Closer),
		users:       make(map[string]*userState),
	}
}

func Configure(cfg Config) error { return Default.Configure(cfg) }

func (m *Manager) Configure(cfg Config) error {
	if !cfg.Enabled {
		m.shutdown("configuration_disabled")
		return nil
	}
	if err := normalizeConfig(&cfg); err != nil {
		return err
	}
	runtime := &runtimeConfig{Config: cfg, httpClient: &http.Client{Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second}}
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	oldConfig := m.config
	if m.cancel != nil {
		m.cancel()
	}
	m.config = runtime
	m.cancel = cancel
	m.nodeDenied = false
	m.mu.Unlock()
	if oldConfig != nil {
		_ = m.callWithConfig(oldConfig, nodeOfflineMethod, map[string]any{"boot_id": m.bootID, "reason": "configuration_reload"}, nil)
	}
	go m.lifecycleLoop(ctx, runtime)
	go m.reportLoop(ctx, time.Duration(cfg.ReportInterval)*time.Second)
	return nil
}

func normalizeConfig(cfg *Config) error {
	if !cfg.Enabled {
		return nil
	}
	cfg.GatewayURL = strings.TrimRight(strings.TrimSpace(cfg.GatewayURL), "/")
	cfg.NodeID = strings.TrimSpace(cfg.NodeID)
	if cfg.GatewayURL == "" || cfg.NodeID == "" || cfg.NodePassword == "" {
		return errors.New("starland-dataplane gateway-url, node-id and node-password are required")
	}
	parsed, err := url.Parse(cfg.GatewayURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("starland-dataplane gateway-url must be an HTTP or HTTPS URL")
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = 10
	}
	if cfg.DenyTTLSeconds <= 0 {
		cfg.DenyTTLSeconds = 30
	}
	if cfg.ReportInterval <= 0 {
		cfg.ReportInterval = 60
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 15
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > 200 {
		cfg.BatchSize = 200
	}
	if cfg.DefaultDownloadLimitMbps <= 0 {
		cfg.DefaultDownloadLimitMbps = 100
	}
	return nil
}

func Shutdown() { Default.shutdown("graceful_shutdown") }

func (m *Manager) shutdown(reason string) {
	m.mu.Lock()
	cfg := m.config
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.config = nil
	m.mu.Unlock()
	if cfg != nil {
		_ = m.callWithConfig(cfg, nodeOfflineMethod, map[string]any{"boot_id": m.bootID, "reason": reason}, nil)
	}
}

func (m *Manager) lifecycleLoop(ctx context.Context, cfg *runtimeConfig) {
	online := false
	retry := time.NewTicker(5 * time.Second)
	heartbeat := time.NewTicker(time.Duration(cfg.HeartbeatInterval) * time.Second)
	defer retry.Stop()
	defer heartbeat.Stop()
	reportOnline := func() {
		if err := m.callWithConfig(cfg, nodeOnlineMethod, map[string]any{"boot_id": m.bootID}, nil); err != nil {
			log.Warnln("[Starland] node online report failed: %v", err)
			return
		}
		online = true
		log.Infoln("[Starland] data-plane node is online")
	}
	reportOnline()
	for {
		select {
		case <-ctx.Done():
			return
		case <-retry.C:
			if !online {
				reportOnline()
			}
		case <-heartbeat.C:
			if !online {
				continue
			}
			if err := m.callWithConfig(cfg, heartbeatMethod, map[string]any{"boot_id": m.bootID}, nil); err != nil {
				log.Warnln("[Starland] node heartbeat failed: %v", err)
			}
		}
	}
}

func ValidateConfig(cfg Config) error { return normalizeConfig(&cfg) }

func Enabled() bool { return Default.Enabled() }

func (m *Manager) Enabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config != nil
}

// Authorize performs the fail-open admission decision, registers the connection
// for later DENY eviction, and starts one asynchronous control-plane check per
// password hash at a time. The returned function must be called on connection close.
func Authorize(passwordSHA256 string, connection io.Closer) (func(), bool) {
	return Default.Authorize(passwordSHA256, connection)
}

func (m *Manager) Authorize(passwordSHA256 string, connection io.Closer) (func(), bool) {
	if !validPasswordSHA256(passwordSHA256) || connection == nil {
		return func() {}, false
	}
	now := time.Now()
	m.mu.Lock()
	if m.config == nil || m.nodeDenied {
		m.mu.Unlock()
		return func() {}, false
	}
	if until, denied := m.denyUntil[passwordSHA256]; denied {
		if now.Before(until) {
			m.mu.Unlock()
			return func() {}, false
		}
		delete(m.denyUntil, passwordSHA256)
	}
	m.nextConnID++
	connectionID := m.nextConnID
	if m.connections[passwordSHA256] == nil {
		m.connections[passwordSHA256] = make(map[uint64]io.Closer)
	}
	m.connections[passwordSHA256][connectionID] = connection
	m.ensureUserLocked(passwordSHA256)
	m.mu.Unlock()

	go func() {
		_, err, _ := m.authGroup.Do(passwordSHA256, func() (any, error) {
			result, err := m.requestAuthorization(passwordSHA256)
			if err != nil {
				return nil, err
			}
			if !result.Allowed {
				m.deny(passwordSHA256)
			} else if result.DownloadLimitMbps == 0 {
				return nil, errors.New("authorization returned an invalid download limit")
			} else {
				m.setDownloadLimit(passwordSHA256, result.DownloadLimitMbps)
			}
			return result.Allowed, nil
		})
		if err != nil {
			log.Warnln("[Starland] asynchronous authorization failed: %v", err)
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() { m.unregisterConnection(passwordSHA256, connectionID) })
	}, true
}

// RegisterTrackedConnection associates an already authorized outbound tracker
// with the user so a later DENY returned by usage reporting can close it.
func (m *Manager) RegisterTrackedConnection(passwordSHA256 string, connection io.Closer) (func(), bool) {
	if !validPasswordSHA256(passwordSHA256) || connection == nil {
		return func() {}, false
	}
	m.mu.Lock()
	if m.config == nil || m.nodeDenied {
		m.mu.Unlock()
		return func() {}, false
	}
	if until, denied := m.denyUntil[passwordSHA256]; denied && time.Now().Before(until) {
		m.mu.Unlock()
		_ = connection.Close()
		return func() {}, false
	}
	m.nextConnID++
	connectionID := m.nextConnID
	if m.connections[passwordSHA256] == nil {
		m.connections[passwordSHA256] = make(map[uint64]io.Closer)
	}
	m.connections[passwordSHA256][connectionID] = connection
	m.ensureUserLocked(passwordSHA256)
	m.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { m.unregisterConnection(passwordSHA256, connectionID) })
	}, true
}

func (m *Manager) unregisterConnection(passwordSHA256 string, connectionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	connections := m.connections[passwordSHA256]
	delete(connections, connectionID)
	if len(connections) == 0 {
		delete(m.connections, passwordSHA256)
	}
}

func (m *Manager) ensureUserLocked(passwordSHA256 string) *userState {
	if user := m.users[passwordSHA256]; user != nil {
		return user
	}
	limitMbps := uint32(m.config.DefaultDownloadLimitMbps)
	user := &userState{downloadLimitMbps: limitMbps, downloadLimiter: newDownloadLimiter(limitMbps)}
	m.users[passwordSHA256] = user
	return user
}

func newDownloadLimiter(limitMbps uint32) *rate.Limiter {
	bytesPerSecond := float64(limitMbps) * 1_000_000 / 8
	return rate.NewLimiter(rate.Limit(bytesPerSecond), downloadBurstSize)
}

func (m *Manager) setDownloadLimit(passwordSHA256 string, limitMbps uint32) {
	if limitMbps == 0 {
		return
	}
	m.mu.RLock()
	user := m.users[passwordSHA256]
	m.mu.RUnlock()
	if user == nil {
		return
	}
	user.limiterMu.Lock()
	user.downloadLimitMbps = limitMbps
	bytesPerSecond := float64(limitMbps) * 1_000_000 / 8
	if user.downloadLimiter == nil {
		user.downloadLimiter = newDownloadLimiter(limitMbps)
	} else {
		now := time.Now()
		user.downloadLimiter.SetLimitAt(now, rate.Limit(bytesPerSecond))
		user.downloadLimiter.SetBurstAt(now, downloadBurstSize)
	}
	user.limiterMu.Unlock()
}

func (m *Manager) deny(passwordSHA256 string) {
	m.mu.Lock()
	if m.config == nil {
		m.mu.Unlock()
		return
	}
	m.denyUntil[passwordSHA256] = time.Now().Add(time.Duration(m.config.DenyTTLSeconds) * time.Second)
	connections := make([]io.Closer, 0, len(m.connections[passwordSHA256]))
	for _, connection := range m.connections[passwordSHA256] {
		connections = append(connections, connection)
	}
	m.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (m *Manager) denyNode() {
	m.mu.Lock()
	if m.config == nil || m.nodeDenied {
		m.mu.Unlock()
		return
	}
	m.nodeDenied = true
	connections := make([]io.Closer, 0)
	for _, userConnections := range m.connections {
		for _, connection := range userConnections {
			connections = append(connections, connection)
		}
	}
	m.mu.Unlock()
	log.Errorln("[Starland] node identity was rejected; data-plane admission is disabled until configuration reload")
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func AddUplink(passwordSHA256 string, size uint64)   { Default.add(passwordSHA256, size, 0) }
func AddDownlink(passwordSHA256 string, size uint64) { Default.add(passwordSHA256, 0, size) }
func WaitDownload(passwordSHA256 string, size int) error {
	return Default.waitDownload(passwordSHA256, size)
}

func (m *Manager) waitDownload(passwordSHA256 string, size int) error {
	if size <= 0 {
		return nil
	}
	m.mu.RLock()
	user := m.users[passwordSHA256]
	m.mu.RUnlock()
	if user == nil {
		return nil
	}
	user.limiterMu.RLock()
	limiter := user.downloadLimiter
	user.limiterMu.RUnlock()
	if limiter == nil {
		return nil
	}
	// Waiting in burst-sized portions only reserves tokens. The network payload
	// remains intact and is returned to the caller with a single read.
	for remaining := size; remaining > 0; {
		chunk := remaining
		if chunk > downloadBurstSize {
			chunk = downloadBurstSize
		}
		if err := limiter.WaitN(context.Background(), chunk); err != nil {
			return err
		}
		remaining -= chunk
	}
	return nil
}

func (m *Manager) add(passwordSHA256 string, uplink, downlink uint64) {
	if uplink == 0 && downlink == 0 {
		return
	}
	m.mu.RLock()
	user := m.users[passwordSHA256]
	enabled := m.config != nil
	m.mu.RUnlock()
	if !enabled || user == nil {
		return
	}
	user.uplink.Add(uplink)
	user.downlink.Add(downlink)
}

func (m *Manager) requestAuthorization(passwordSHA256 string) (authorizeResponse, error) {
	var result authorizeResponse
	if err := m.call(authorizeMethod, map[string]any{"password_sha256": passwordSHA256}, &result); err != nil {
		if authoritativeNodeError(err) {
			m.denyNode()
		}
		return authorizeResponse{}, err
	}
	return result, nil
}

func (m *Manager) reportLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.reportUsage(); err != nil {
				log.Warnln("[Starland] usage report failed: %v", err)
			}
		}
	}
}

func (m *Manager) reportUsage() error {
	m.mu.RLock()
	if m.config == nil {
		m.mu.RUnlock()
		return nil
	}
	batchSize := m.config.BatchSize
	items := make([]usageItem, 0, len(m.users))
	for passwordSHA256, user := range m.users {
		uplink, downlink := user.uplink.Load(), user.downlink.Load()
		if uplink != user.confirmedUplink.Load() || downlink != user.confirmedDownlink.Load() {
			items = append(items, usageItem{PasswordSHA256: passwordSHA256, UplinkTotalBytes: uplink, DownlinkTotalBytes: downlink})
		}
	}
	m.mu.RUnlock()
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool { return items[i].PasswordSHA256 < items[j].PasswordSHA256 })
	sequence := atomic.AddUint64(&m.sequence, 1)
	for start := 0; start < len(items); start += batchSize {
		end := start + batchSize
		if end > len(items) {
			end = len(items)
		}
		var response reportUsageResponse
		if err := m.call(reportUsageMethod, reportUsageRequest{BootID: m.bootID, Sequence: sequence, Items: items[start:end]}, &response); err != nil {
			if authoritativeNodeError(err) {
				m.denyNode()
			}
			return err
		}
		for _, decision := range response.Decisions {
			if decision.Accepted {
				m.confirmReported(decision.PasswordSHA256, items[start:end])
			}
			if !decision.Accepted {
				log.Errorln("[Starland] usage report was rejected: credential=%s reason=%s", shortCredential(decision.PasswordSHA256), decision.DenyReason)
			}
			if !decision.Allowed && authoritativeDenyReason(decision.DenyReason) {
				m.confirmReported(decision.PasswordSHA256, items[start:end])
				m.deny(decision.PasswordSHA256)
			}
		}
	}
	return nil
}

func (m *Manager) confirmReported(passwordSHA256 string, items []usageItem) {
	var reported *usageItem
	for index := range items {
		if items[index].PasswordSHA256 == passwordSHA256 {
			reported = &items[index]
			break
		}
	}
	if reported == nil {
		return
	}
	m.mu.Lock()
	user := m.users[passwordSHA256]
	if user != nil {
		user.confirmedUplink.Store(reported.UplinkTotalBytes)
		user.confirmedDownlink.Store(reported.DownlinkTotalBytes)
	}
	m.mu.Unlock()
}

type httpStatusError struct{ status int }

func (e *httpStatusError) Error() string { return fmt.Sprintf("gateway returned HTTP %d", e.status) }

func authoritativeNodeError(err error) bool {
	var statusErr *httpStatusError
	return errors.As(err, &statusErr) && (statusErr.status == http.StatusUnauthorized || statusErr.status == http.StatusForbidden)
}

func (m *Manager) call(method string, params any, result any) error {
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg == nil {
		return errors.New("Starland data-plane integration is disabled")
	}
	return m.callWithConfig(cfg, method, params, result)
}

func (m *Manager) callWithConfig(cfg *runtimeConfig, method string, params any, result any) error {
	body, err := json.Marshal(gatewayRequest{Com: gatewayCom{NodeID: cfg.NodeID, NodePassword: cfg.NodePassword}, Req: gatewayCall{Module: moduleName, Method: method, Params: params}})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, cfg.GatewayURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cfg.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return &httpStatusError{status: resp.StatusCode}
	}
	var envelope gatewayResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&envelope); err != nil {
		return err
	}
	if envelope.Code != 0 {
		return fmt.Errorf("gateway returned code %d: %s", envelope.Code, envelope.Message)
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(envelope.Data, result)
}

func validPasswordSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func shortCredential(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

func authoritativeDenyReason(reason string) bool {
	switch reason {
	case "credential_invalid", "credential_revoked", "account_disabled", "no_entitlement", "traffic_exhausted":
		return true
	default:
		return false
	}
}

type gatewayRequest struct {
	Com gatewayCom  `json:"com"`
	Req gatewayCall `json:"req"`
}

type gatewayCom struct {
	NodeID       string `json:"node_id"`
	NodePassword string `json:"node_password"`
}

type gatewayCall struct {
	Module string `json:"module"`
	Method string `json:"method"`
	Params any    `json:"params"`
}

type gatewayResponse struct {
	Code    int             `json:"code"`
	Message string          `json:"msg"`
	Data    json.RawMessage `json:"data"`
}

type authorizeResponse struct {
	Allowed           bool   `json:"allowed"`
	DownloadLimitMbps uint32 `json:"download_limit_mbps"`
}

type usageItem struct {
	PasswordSHA256     string `json:"password_sha256"`
	UplinkTotalBytes   uint64 `json:"uplink_total_bytes"`
	DownlinkTotalBytes uint64 `json:"downlink_total_bytes"`
}

type reportUsageRequest struct {
	BootID   string      `json:"boot_id"`
	Sequence uint64      `json:"sequence"`
	Items    []usageItem `json:"items"`
}

type usageDecision struct {
	PasswordSHA256 string `json:"password_sha256"`
	Accepted       bool   `json:"accepted"`
	Allowed        bool   `json:"allowed"`
	DenyReason     string `json:"deny_reason"`
}

type reportUsageResponse struct {
	Decisions []usageDecision `json:"decisions"`
}
