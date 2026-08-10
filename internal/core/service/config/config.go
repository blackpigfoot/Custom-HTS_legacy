package config

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"Custom-HTS/internal/core/pkg/logger"
)

var (
	ErrConfigNotFound   = errors.New("config file not found")
	ErrAccountNotFound  = errors.New("account not found in config")
	ErrDuplicateAccount = errors.New("duplicate account ID")
	ErrMissingKeySalt   = errors.New("KeySalt is empty: call NewSalt() and set Config.KeySalt before Save()")
)

// AssetType — 자산 유형 식별자.
// Gateway에서 주식/코인을 분기할 때 사용하는 핵심 타입.
//
// 왜 필요한가:
//   - 주식: 어떤 증권사든 삼성전자 005930 가격은 동일 → SmartRouter가 계좌 자동 선택 가능
//   - 코인: Binance와 Upbit의 BTC 가격이 다름 → 거래소를 반드시 지정해야 함
type AssetType string

const (
	AssetStock  AssetType = "stock"
	AssetCrypto AssetType = "crypto"
)

// BrokerType — 브로커/거래소 식별자.
// 계좌 등록 시 어떤 Exchange 구현체를 생성할지 결정하는 키.
type BrokerType string

const (
	BrokerKIS     BrokerType = "kis"
	BrokerEBest   BrokerType = "ebest"
	BrokerBinance BrokerType = "binance"
	BrokerUpbit   BrokerType = "upbit"
	BrokerBithumb BrokerType = "bithumb"
)

func (b BrokerType) String() string { return string(b) }

// AssetTypeOf — 브로커 타입에서 자산 유형 추론.
func AssetTypeOf(b BrokerType) AssetType {
	switch b {
	case BrokerKIS, BrokerEBest:
		return AssetStock
	case BrokerBinance, BrokerUpbit, BrokerBithumb:
		return AssetCrypto
	default:
		return ""
	}
}

// Config — 전체 설정 구조체.
//
// 설정 파일 구조:
//
//	{
//	  "key_salt": "base64...",
//	  "accounts": [...],
//	  "server": {...},
//	  "data_dir": "..."
//	}
type Config struct {
	// KeySalt — scrypt 키 파생에 사용하는 salt (base64 인코딩).
	// NewSalt()로 생성하여 최초 Save() 전에 반드시 설정해야 함.
	// 한번 설정된 salt는 변경하면 안 됨 (변경 시 기존 암호화 데이터 복호화 불가).
	KeySalt string `json:"key_salt"`

	// Accounts — 등록된 계좌 목록.
	Accounts []AccountConfig `json:"accounts"`

	// Server — API 서버 설정.
	Server ServerConfig `json:"server"`

	// DataDir — 데이터 저장 디렉토리.
	// 빈 문자열이면 기본값 ~/.trading-platform/ 사용.
	DataDir string `json:"data_dir"`

	// Debug — 로깅/디버그 설정.
	// 앱 시작 시 한 번 읽어 불변으로 사용. 변경 시 재시작 필요.
	Debug logger.DebugConfig `json:"debug"`

	// configPath — 이 설정 파일의 경로. JSON에는 포함되지 않음.
	configPath string

	// mu — 런타임 계좌 추가/삭제 보호.
	mu sync.RWMutex
}

// ServerConfig — 서버 관련 설정.
type ServerConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// AccountConfig — 계좌 설정 구조체.
type AccountConfig struct {
	ID          string           `json:"id"`
	Broker      BrokerType       `json:"broker"`
	Enabled     bool             `json:"enabled"`
	Credentials CredentialConfig `json:"credentials"`
	BaseURL     string           `json:"base_url"`
	IsPaper     bool             `json:"ispaper"`
}

// CredentialConfig — API 인증 정보.
//
// Encrypted 필드:
//   - Save() 시: 평문 → 암호화 + true로 설정
//   - DecryptCredentials() 후: 복호화 + false로 설정
//   - 메모리에서는 항상 평문 상태로 사용
type CredentialConfig struct {
	APIKey    string `json:"api_key"`
	APISecret string `json:"api_secret"`
	AccountNo string `json:"account_no,omitempty"`
	Encrypted bool   `json:"encrypted"`
}

// ProxyConfig — 프록시 설정.
type ProxyConfig struct {
	URL string `json:"url"`
}

// Load — JSON 파일에서 설정 로드 (credential은 암호화된 상태로 유지).
//
// 반환된 Config의 credential은 암호화 상태.
// DecryptCredentials(key) 호출 전에는 API 키를 사용하면 안 됨.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrConfigNotFound, path)
		}
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg.configPath = path
	return &cfg, nil
}

// LoadAndDecrypt — JSON 파일 로드 + credential 복호화.
//
// encKey는 DeriveKeyFromConfig(password, cfg)로 생성.
//
// 사용:
//
//	cfg, _ := config.Load(path)
//	key, _ := config.DeriveKeyFromConfig(password, cfg)
//	cfg.DecryptCredentials(key)
//
// 또는 한 번에:
//
//	cfg, _ := config.LoadAndDecrypt(path, key)
func LoadAndDecrypt(path string, encKey []byte) (*Config, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}
	if err := cfg.DecryptCredentials(encKey); err != nil {
		return nil, err
	}
	return cfg, nil
}

// DecryptCredentials — 로드된 Config의 credential을 복호화 (in-place).
//
// Load() 후 DeriveKeyFromConfig()로 키를 파생한 다음 호출.
// 이미 복호화된 계좌(Encrypted==false)는 건너뜀.
func (c *Config) DecryptCredentials(encKey []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.Accounts {
		cred := &c.Accounts[i].Credentials
		if !cred.Encrypted {
			continue
		}
		var err error
		cred.APIKey, err = Decrypt(cred.APIKey, encKey)
		if err != nil {
			return fmt.Errorf("decrypting api_key for %s: %w", c.Accounts[i].ID, err)
		}
		cred.APISecret, err = Decrypt(cred.APISecret, encKey)
		if err != nil {
			return fmt.Errorf("decrypting api_secret for %s: %w", c.Accounts[i].ID, err)
		}
		cred.Encrypted = false
	}
	return nil
}

// Save — 설정을 JSON 파일로 저장 (credential은 암호화하여 저장).
//
// 보안:
//   - 원본 Config의 credential은 변경하지 않음 (clone에서 암호화)
//   - 파일 권한 0600 (소유자만 읽기/쓰기)
//
// KeySalt가 비어있으면 에러 반환.
// 최초 저장 전 반드시:
//
//	salt, _ := config.NewSalt()
//	cfg.KeySalt = base64.StdEncoding.EncodeToString(salt)
//	key, _ := config.DeriveKey(password, salt)
//	cfg.Save(path, key)
func (c *Config) Save(path string, encKey []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// credential이 있는데 KeySalt가 없으면 키 파생이 불가능해지므로 차단.
	if c.KeySalt == "" {
		for _, acc := range c.Accounts {
			if acc.Credentials.APIKey != "" || acc.Credentials.APISecret != "" {
				return ErrMissingKeySalt
			}
		}
	}

	clone := c.clone()

	for i := range clone.Accounts {
		cred := &clone.Accounts[i].Credentials
		if cred.Encrypted || (cred.APIKey == "" && cred.APISecret == "") {
			continue
		}
		var err error
		if cred.APIKey != "" {
			cred.APIKey, err = Encrypt(cred.APIKey, encKey)
			if err != nil {
				return fmt.Errorf("encrypting api_key for %s: %w", clone.Accounts[i].ID, err)
			}
		}
		if cred.APISecret != "" {
			cred.APISecret, err = Encrypt(cred.APISecret, encKey)
			if err != nil {
				return fmt.Errorf("encrypting api_secret for %s: %w", clone.Accounts[i].ID, err)
			}
		}
		cred.Encrypted = true
	}

	data, err := json.MarshalIndent(clone, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating config dir: %w", err)
	}

	return os.WriteFile(path, data, 0o600)
}

// Default — 기본 설정 반환. 최초 실행 시 사용.
func Default() *Config {
	return &Config{
		Accounts: []AccountConfig{},
		Server: ServerConfig{
			Host: "127.0.0.1",
			Port: 8080,
		},
		Debug: logger.DebugConfig{
			LogLevel: "info",
		},
	}
}

// GetAccount — ID로 계좌 설정 조회.
func (c *Config) GetAccount(id string) (*AccountConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for i := range c.Accounts {
		if c.Accounts[i].ID == id {
			return &c.Accounts[i], nil
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, id)
}

// AddAccount — 계좌 추가. ID 중복 시 에러 반환.
func (c *Config) AddAccount(acc AccountConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, existing := range c.Accounts {
		if existing.ID == acc.ID {
			return fmt.Errorf("%w: %s", ErrDuplicateAccount, acc.ID)
		}
	}
	c.Accounts = append(c.Accounts, acc)
	return nil
}

// RemoveAccount — ID로 계좌 제거.
func (c *Config) RemoveAccount(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, acc := range c.Accounts {
		if acc.ID == id {
			c.Accounts = append(c.Accounts[:i], c.Accounts[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrAccountNotFound, id)
}

// EnabledAccounts — 활성화된 계좌만 반환.
func (c *Config) EnabledAccounts() []AccountConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []AccountConfig
	for _, acc := range c.Accounts {
		if acc.Enabled {
			result = append(result, acc)
		}
	}
	return result
}

// ResolveDataDir — 데이터 디렉토리 경로 확정.
func (c *Config) ResolveDataDir() string {
	if c.DataDir != "" {
		return c.DataDir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".trading-platform")
}

func (c *Config) clone() *Config {
	clone := &Config{
		KeySalt:    c.KeySalt,
		Server:     c.Server,
		DataDir:    c.DataDir,
		Debug:      c.Debug,
		configPath: c.configPath,
	}
	clone.Accounts = make([]AccountConfig, len(c.Accounts))
	copy(clone.Accounts, c.Accounts)
	return clone
}

// SetupKeySalt — 최초 설정 시 salt 생성 및 설정 헬퍼.
//
// 내부적으로 NewSalt()를 호출하고 cfg.KeySalt에 설정.
// 반환된 salt로 DeriveKey를 호출해 encKey를 생성.
//
// 사용:
//
//	salt, err := cfg.SetupKeySalt()
//	key, err := config.DeriveKey(password, salt)
//	cfg.Save(path, key)
func (c *Config) SetupKeySalt() ([]byte, error) {
	salt, err := NewSalt()
	if err != nil {
		return nil, err
	}
	c.KeySalt = base64.StdEncoding.EncodeToString(salt)
	return salt, nil
}
