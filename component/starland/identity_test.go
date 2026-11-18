package starland

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validIdentity = "node_id: relay-1\nnode_password: secret-value\ngateway_url: http://127.0.0.1:12001\n"

func writeIdentity(t *testing.T, mode os.FileMode, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "node-identity")
	if err := os.WriteFile(p, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	// 绕开 umask，确保权限就是用例声明的值。
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestApplyNodeIdentityIsSingleSourceOfTruth(t *testing.T) {
	p := writeIdentity(t, 0o600, validIdentity)
	cfg := &Config{Enabled: true, NodeID: "from-template", NodePassword: "from-template", GatewayURL: "http://old"}
	if err := ApplyNodeIdentity(cfg, p); err != nil {
		t.Fatal(err)
	}
	if cfg.NodeID != "relay-1" || cfg.NodePassword != "secret-value" || cfg.GatewayURL != "http://127.0.0.1:12001" {
		t.Fatal("identity file did not override template values")
	}
}

func TestApplyNodeIdentityFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		path func(t *testing.T) string
	}{
		{"missing file", func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent") }},
		{"malformed yaml", func(t *testing.T) string { return writeIdentity(t, 0o600, "node_id: [unclosed\n") }},
		{"incomplete fields", func(t *testing.T) string { return writeIdentity(t, 0o600, "node_id: x\n") }},
		{"loose permissions", func(t *testing.T) string { return writeIdentity(t, 0o644, validIdentity) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &Config{Enabled: true, NodeID: "old", NodePassword: "old", GatewayURL: "http://old"}
			err := ApplyNodeIdentity(cfg, c.path(t))
			if err == nil {
				t.Fatal("expected fail-closed error")
			}
			if cfg.NodePassword != "old" {
				t.Error("config mutated despite failure")
			}
			if containsSecret(err.Error()) {
				t.Error("error message leaked credential value")
			}
		})
	}
}

func containsSecret(s string) bool {
	return strings.Contains(s, "secret-value")
}
