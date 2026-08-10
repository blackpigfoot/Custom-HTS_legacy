package auth2

// -------------------------------------------------------
// 토큰 설정 구조체 및 관련 인터페이스 정의
// -------------------------------------------------------

import (
	"context"
	"net/http"
	"time"
)

type TokenResPonse struct {
	Header http.Header
	Body   []byte
}

type TokenSource interface {
	Token(ctx context.Context) (*Token, error)
}

type Token struct {
	// 실제 토큰 값
	AccessToken string `json:"access_token"`

	// 만료 시간
	ExpiresAt time.Time `json:"expires_at"`

	// 토큰 타입 (예: "Bearer")
	Type string `json:"token_type"`
}

// AuthHeader 헤더에 인증 정보 추가
func (t *Token) AuthHeader(r *http.Request) {
	r.Header.Set("Authorization", t.Type+" "+t.AccessToken)
}

func (t *Token) IsValid() bool {
	return t != nil && t.ExpiresAt.After(time.Now().Add(10*time.Minute))
}
