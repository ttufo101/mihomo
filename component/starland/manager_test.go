package starland

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testCloser struct {
	once   sync.Once
	closed chan struct{}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func (c *testCloser) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestAuthorizeFailsOpenThenCachesDeny(t *testing.T) {
	var calls atomic.Int32
	requestErr := make(chan error, 1)
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		var request gatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			requestErr <- err
		} else if request.Com.NodeID != "relay-test" || request.Com.NodePassword != "node-secret" || request.Req.Method != authorizeMethod {
			requestErr <- &unexpectedRequestError{}
		} else {
			requestErr <- nil
		}
		body := `{"code":0,"msg":"ok","data":{"allowed":false,"deny_reason":"no_entitlement"}}`
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(body))}, nil
	})

	manager := newManager()
	require.NoError(t, manager.Configure(Config{Enabled: true, GatewayURL: "https://gateway.example/startlandapi", NodeID: "relay-test", NodePassword: "node-secret", DenyTTLSeconds: 30, ReportInterval: 3600}))
	defer manager.Configure(Config{})
	manager.mu.Lock()
	manager.config.httpClient.Transport = transport
	manager.mu.Unlock()
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	connection := &testCloser{closed: make(chan struct{})}
	release, allowed := manager.Authorize(hash, connection)
	require.True(t, allowed)
	defer release()
	select {
	case <-connection.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("DENY did not close the active connection")
	}
	require.NoError(t, <-requestErr)
	_, allowed = manager.Authorize(hash, &testCloser{closed: make(chan struct{})})
	require.False(t, allowed)
	require.Equal(t, int32(1), calls.Load())
}

func TestNodeUnauthorizedDisablesAdmissionUntilReload(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusUnauthorized, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(`{}`))}, nil
	})
	manager := newManager()
	require.NoError(t, manager.Configure(Config{Enabled: true, GatewayURL: "https://gateway.example/startlandapi", NodeID: "relay-test", NodePassword: "wrong", ReportInterval: 3600}))
	defer manager.Configure(Config{})
	manager.mu.Lock()
	manager.config.httpClient.Transport = transport
	manager.mu.Unlock()
	hash := "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	connection := &testCloser{closed: make(chan struct{})}
	_, allowed := manager.Authorize(hash, connection)
	require.True(t, allowed)
	select {
	case <-connection.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("node rejection did not close the active connection")
	}
	_, allowed = manager.Authorize(hash, &testCloser{closed: make(chan struct{})})
	require.False(t, allowed)
	require.NoError(t, manager.Configure(Config{Enabled: true, GatewayURL: "https://gateway.example/startlandapi", NodeID: "relay-test", NodePassword: "new-secret", ReportInterval: 3600}))
	manager.mu.RLock()
	recovered := !manager.nodeDenied
	manager.mu.RUnlock()
	require.True(t, recovered)
}

func TestNodeServiceUnavailableKeepsAdmissionEnabled(t *testing.T) {
	var calls atomic.Int32
	requestDone := make(chan struct{}, 1)
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		if call == 1 {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(`{}`))}, nil
		}
		requestDone <- struct{}{}
		body := `{"code":0,"msg":"ok","data":{"allowed":true,"download_limit_mbps":50}}`
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(body))}, nil
	})
	manager := newManager()
	require.NoError(t, manager.Configure(Config{Enabled: true, GatewayURL: "https://gateway.example/startlandapi", NodeID: "relay-test", NodePassword: "node-secret", ReportInterval: 3600}))
	defer manager.Configure(Config{})
	manager.mu.Lock()
	manager.config.httpClient.Transport = transport
	manager.mu.Unlock()

	hash := "1223456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	_, err := manager.requestAuthorization(hash)
	require.Error(t, err)
	manager.mu.RLock()
	require.False(t, manager.nodeDenied)
	manager.mu.RUnlock()

	connection := &testCloser{closed: make(chan struct{})}
	release, allowed := manager.Authorize(hash, connection)
	require.True(t, allowed)
	defer release()
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("authorization did not recover after service became available")
	}
	select {
	case <-connection.closed:
		t.Fatal("temporary node verification failure closed the active connection")
	default:
	}
	require.Equal(t, int32(2), calls.Load())
}

func TestConfirmedDisconnectedUserCountersRemainMonotonic(t *testing.T) {
	manager := newManager()
	require.NoError(t, manager.Configure(Config{Enabled: true, GatewayURL: "https://gateway.example/startlandapi", NodeID: "relay-test", NodePassword: "secret", ReportInterval: 3600}))
	defer manager.Configure(Config{})
	hash := "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	closer := &testCloser{closed: make(chan struct{})}
	release, ok := manager.RegisterTrackedConnection(hash, closer)
	require.True(t, ok)
	manager.add(hash, 10, 20)
	release()
	manager.confirmReported(hash, []usageItem{{PasswordSHA256: hash, UplinkTotalBytes: 10, DownlinkTotalBytes: 20}})
	manager.mu.RLock()
	user, exists := manager.users[hash]
	manager.mu.RUnlock()
	require.True(t, exists)
	require.Equal(t, uint64(10), user.uplink.Load())
	require.Equal(t, uint64(20), user.downlink.Load())

	secondCloser := &testCloser{closed: make(chan struct{})}
	secondRelease, ok := manager.RegisterTrackedConnection(hash, secondCloser)
	require.True(t, ok)
	manager.add(hash, 5, 7)
	secondRelease()
	require.Equal(t, uint64(15), user.uplink.Load())
	require.Equal(t, uint64(27), user.downlink.Load())
}

func TestAuthorizationUpdatesSharedDownloadLimit(t *testing.T) {
	requestDone := make(chan struct{}, 1)
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestDone <- struct{}{}
		body := `{"code":0,"msg":"ok","data":{"allowed":true,"download_limit_mbps":50}}`
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(body))}, nil
	})
	manager := newManager()
	require.NoError(t, manager.Configure(Config{Enabled: true, GatewayURL: "https://gateway.example/startlandapi", NodeID: "relay-test", NodePassword: "secret", ReportInterval: 3600, DefaultDownloadLimitMbps: 100}))
	defer manager.Configure(Config{})
	manager.mu.Lock()
	manager.config.httpClient.Transport = transport
	manager.mu.Unlock()
	hash := "3123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	release, allowed := manager.Authorize(hash, &testCloser{closed: make(chan struct{})})
	require.True(t, allowed)
	defer release()
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("authorization request did not complete")
	}
	require.Eventually(t, func() bool {
		manager.mu.RLock()
		user := manager.users[hash]
		manager.mu.RUnlock()
		if user == nil {
			return false
		}
		user.limiterMu.RLock()
		defer user.limiterMu.RUnlock()
		return user.downloadLimitMbps == 50
	}, 2*time.Second, 10*time.Millisecond)
}

type unexpectedRequestError struct{}

func (*unexpectedRequestError) Error() string { return "unexpected Gateway request" }

func TestAuthoritativeDenyReason(t *testing.T) {
	require.True(t, authoritativeDenyReason("traffic_exhausted"))
	require.False(t, authoritativeDenyReason("processing_failed"))
	require.False(t, authoritativeDenyReason(""))
}

func TestValidateConfig(t *testing.T) {
	require.NoError(t, ValidateConfig(Config{}))
	require.Error(t, ValidateConfig(Config{Enabled: true, GatewayURL: "ftp://example.com", NodeID: "node", NodePassword: "secret"}))
	require.NoError(t, ValidateConfig(Config{Enabled: true, GatewayURL: "https://example.com/startlandapi", NodeID: "node", NodePassword: "secret"}))
}
