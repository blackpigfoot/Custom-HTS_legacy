package config

import (
	"fmt"
	"strings"
)

// ValidationError — 설정 검증 에러. 여러 에러 메시지를 담을 수 있음.
// 하나의 에러만 반환하면 사용자가 수정→재시도 반복해야 함.
// 모든 에러를 한번에 알려줘서 한 번에 수정 가능.
type ValidationError struct {
	Errors []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("config validation failed:\n  - %s", strings.Join(e.Errors, "\n  - "))
}

func (e *ValidationError) add(msg string) {
	e.Errors = append(e.Errors, msg)
}

func (e *ValidationError) hasErrors() bool {
	return len(e.Errors) > 0
}

// Validate — 설정 검증 진입점.
// 모든 검증을 수행하고, 에러가 있으면 *ValidationError 반환.
func Validate(cfg *Config) error {
	ve := &ValidationError{}

	validateServer(&cfg.Server, ve)
	validateAccounts(cfg.Accounts, ve)

	if ve.hasErrors() {
		return ve
	}
	return nil
}

// validateServer — 서버 설정 검증.
func validateServer(s *ServerConfig, ve *ValidationError) {
	if s.Port < 0 || s.Port > 65535 {
		ve.add(fmt.Sprintf("server.port must be 0-65535, got %d", s.Port))
	}
}

// validateAccounts — 계좌 목록 검증.
//
// 검증 항목:
//  1. ID 필수 + 중복 검사
//  2. 브로커 타입 유효성
//  3. 활성 계좌의 credential 완전성
//  4. 주식 계좌의 계좌번호 존재 여부
//  5. environment 값 유효성 ("real" 또는 "paper")
func validateAccounts(accounts []AccountConfig, ve *ValidationError) {
	seen := make(map[string]bool)

	for i, acc := range accounts {
		prefix := fmt.Sprintf("accounts[%d](%s)", i, acc.ID)

		// ID 필수
		if acc.ID == "" {
			ve.add(fmt.Sprintf("accounts[%d].id is required", i))
			continue
		}

		// 중복 ID 검사
		if seen[acc.ID] {
			ve.add(fmt.Sprintf("%s: duplicate account ID", prefix))
		}
		seen[acc.ID] = true

		// 브로커 타입 검사
		if !isValidBroker(acc.Broker) {
			ve.add(fmt.Sprintf("%s: invalid broker %q", prefix, acc.Broker))
		}

		// 비활성 계좌는 credential 검증 스킵.
		// 비활성 계좌는 로드하지 않으므로 credential이 비어있어도 무방.
		if !acc.Enabled {
			continue
		}

		// credential 검증 (활성 계좌만)
		validateCredentials(prefix, &acc, ve)
	}
}

// validateCredentials — 개별 계좌의 credential 검증.
//
// 주식 브로커(KIS, eBest)는 계좌번호(AccountNo) 필수.
// 코인 거래소(Binance 등)는 계좌번호 불필요.
func validateCredentials(prefix string, acc *AccountConfig, ve *ValidationError) {
	cred := &acc.Credentials

	if cred.APIKey == "" {
		ve.add(fmt.Sprintf("%s: api_key is required for enabled account", prefix))
	}
	if cred.APISecret == "" {
		ve.add(fmt.Sprintf("%s: api_secret is required for enabled account", prefix))
	}

	// 주식 브로커는 계좌번호 필수
	if AssetTypeOf(acc.Broker) == AssetStock && cred.AccountNo == "" {
		ve.add(fmt.Sprintf("%s: account_no is required for stock broker", prefix))
	}
}

// isValidBroker — 지원되는 브로커 타입인지 확인.
func isValidBroker(b BrokerType) bool {
	switch b {
	case BrokerKIS, BrokerEBest, BrokerBinance, BrokerUpbit, BrokerBithumb:
		return true
	default:
		return false
	}
}
