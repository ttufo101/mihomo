package starland

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

// DefaultNodeIdentityPath 是本机身份文件的唯一事实源。
// 它由节点纳管流程写入（裸机阶段、安装任何应用之前），不属于任何应用目录，
// 因此发布中心覆盖二进制与 conf/config.yaml 时永远碰不到它。
const DefaultNodeIdentityPath = "/etc/starland/node-identity"

type nodeIdentityFile struct {
	NodeID       string `yaml:"node_id"`
	NodePassword string `yaml:"node_password"`
	GatewayURL   string `yaml:"gateway_url"`
}

// ApplyNodeIdentity 从本机身份文件读取凭据并覆盖内存配置。
// path 为空时使用 DefaultNodeIdentityPath（单测可传临时路径）。
// 缺失 / 格式错 / 权限不是 0600 / 字段不全一律返回错误，由调用方拒绝启动；
// 错误信息绝不携带任何凭据值。
func ApplyNodeIdentity(cfg *Config, path string) error {
	if cfg == nil {
		return errors.New("starland node identity: nil config")
	}
	if path == "" {
		path = DefaultNodeIdentityPath
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("starland node identity unavailable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("starland node identity is not a regular file")
	}
	if info.Mode().Perm() != 0o600 {
		return errors.New("starland node identity must be mode 0600")
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("starland node identity unreadable: %w", err)
	}
	var f nodeIdentityFile
	if err := yaml.Unmarshal(buf, &f); err != nil {
		return fmt.Errorf("starland node identity is malformed: %w", err)
	}
	f.NodeID = strings.TrimSpace(f.NodeID)
	f.NodePassword = strings.TrimSpace(f.NodePassword)
	f.GatewayURL = strings.TrimSpace(f.GatewayURL)
	if f.NodeID == "" || f.NodePassword == "" || f.GatewayURL == "" {
		return errors.New("starland node identity is incomplete (node_id/node_password/gateway_url required)")
	}
	// 身份文件是唯一事实源：模板里即便残留同名字段也一律覆盖，避免两套来源。
	cfg.NodeID = f.NodeID
	cfg.NodePassword = f.NodePassword
	cfg.GatewayURL = f.GatewayURL
	return nil
}
