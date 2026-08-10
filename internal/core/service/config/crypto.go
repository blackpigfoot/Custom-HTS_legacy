package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"

	"golang.org/x/crypto/scrypt"
)

// 설정 파일에 API 키를 저장할 때 사용하는 암호화 유틸리티.
//
// 왜 필요한가:
//   - API 키가 평문으로 파일에 저장되면 보안 위험
//   - 서버가 24시간 상주하므로 설정 파일이 디스크에 오래 존재
//   - Git에 실수로 커밋해도 평문 유출 방지
//
// AES-GCM을 선택한 이유:
//   - AES-CBC: 패딩 오라클 공격 취약, MAC 별도 필요
//   - AES-GCM: AEAD (인증+암호화 통합), 표준 권장, Go 표준 라이브러리 지원
//
// 저장 형식:
//   base64(nonce(12byte) + ciphertext + tag(16byte))
//
// 키 파생:
//   사용자 비밀번호 + salt → scrypt → 32바이트 AES-256 키
//
// scrypt를 선택한 이유 (SHA-256 대비):
//   - SHA-256은 GPU로 초당 수십억 회 연산 가능 → 브루트포스 취약
//   - scrypt는 메모리+연산 비용이 높아 GPU 병렬화가 사실상 불가능
//   - 동일 비밀번호라도 salt가 달라 레인보우 테이블 공격 차단

const (
	// scrypt 파라미터.
	// N=32768: CPU/메모리 비용. 높을수록 안전하지만 느림.
	//          32768(2^15)은 인터랙티브 로그인 수준의 표준값.
	// r=8, p=1: 블록 크기와 병렬화 인수. RFC 7914 권장값.
	scryptN   = 1 << 15 // 32768
	scryptR   = 8
	scryptP   = 1
	scryptLen = 32 // AES-256 키 길이
)

// NewSalt — 암호학적으로 안전한 16바이트 랜덤 salt 생성.
//
// 최초 설정 저장 시 한 번 생성하여 Config.KeySalt에 저장.
// 이후 DeriveKey 호출마다 동일 salt를 재사용해야 동일 키가 파생됨.
//
// 사용:
//
//	salt, err := config.NewSalt()
//	cfg.KeySalt = base64.StdEncoding.EncodeToString(salt)
func NewSalt() ([]byte, error) {
	salt := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("generating salt: %w", err)
	}
	return salt, nil
}

// DeriveKey — 비밀번호와 salt로 AES-256 키 파생 (scrypt).
//
// 동일 비밀번호 + 동일 salt → 항상 동일 키 (결정적).
// salt 없이는 키 파생 불가 → 레인보우 테이블 차단.
//
// 사용:
//
//	key, err := config.DeriveKey(password, salt)
//	cfg.Save("config.json", key)
//
// salt는 Config.KeySalt에 base64로 저장됨.
// LoadAndDecrypt 전에 Load로 salt를 먼저 읽어야 함.
func DeriveKey(password string, salt []byte) ([]byte, error) {
	key, err := scrypt.Key([]byte(password), salt, scryptN, scryptR, scryptP, scryptLen)
	if err != nil {
		return nil, fmt.Errorf("deriving key: %w", err)
	}
	return key, nil
}

// DeriveKeyFromConfig — Config에 저장된 salt를 사용해 키 파생.
//
// Load()로 config를 먼저 읽은 뒤 이 함수로 키를 파생하면
// LoadAndDecrypt에 전달할 수 있음.
//
// 사용:
//
//	cfg, _ := config.Load(path)
//	key, err := config.DeriveKeyFromConfig(password, cfg)
//	cfg.DecryptCredentials(key)
func DeriveKeyFromConfig(password string, cfg *Config) ([]byte, error) {
	if cfg.KeySalt == "" {
		return nil, fmt.Errorf("KeySalt not set in config: was this config created with an older version?")
	}
	salt, err := base64.StdEncoding.DecodeString(cfg.KeySalt)
	if err != nil {
		return nil, fmt.Errorf("decoding KeySalt: %w", err)
	}
	return DeriveKey(password, salt)
}

// Encrypt — 평문을 AES-256-GCM으로 암호화하여 base64로 인코딩.
//
// 반환값: base64 인코딩된 문자열 (JSON 저장 가능).
//
// 내부 동작:
//  1. 12바이트 랜덤 nonce 생성 (crypto/rand, 암호학적 안전)
//  2. AES-256-GCM으로 암호화 (nonce + plaintext → ciphertext + tag)
//  3. nonce + ciphertext를 결합하여 base64 인코딩
//
// 같은 평문을 두 번 암호화해도 nonce가 달라 결과가 다름.
// key는 반드시 32바이트 (AES-256). DeriveKey()로 생성.
func Encrypt(plaintext string, key []byte) (string, error) {
	if len(key) != 32 {
		return "", fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("creating cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("creating GCM: %w", err)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generating nonce: %w", err)
	}

	// 결과: nonce(12) + ciphertext + tag(16)
	ciphertext := aesGCM.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt — Encrypt()로 암호화된 문자열을 복호화.
//
// 잘못된 키로 복호화 시도하면 GCM 인증 실패로 에러 반환.
// AES-CBC와 달리 "잘못된 키로 복호화된 쓰레기 데이터"가 나오지 않음이 보장됨.
func Decrypt(encoded string, key []byte) (string, error) {
	if len(key) != 32 {
		return "", fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("creating cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("creating GCM: %w", err)
	}

	nonceSize := aesGCM.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decryption failed (wrong password?): %w", err)
	}

	return string(plaintext), nil
}
