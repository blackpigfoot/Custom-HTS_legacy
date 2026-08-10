package auth2

// -------------------------------------------------------
// Token Transport
// -------------------------------------------------------

import (
	"net/http"
)

// Transport 토큰 전송용 라운드트리퍼
// 요청 시마다 토큰을 가져와서 Authorization 헤더에 추가합니다.
type Transport struct {
	Base        http.RoundTripper
	TokenSource TokenSource
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	token, err := t.TokenSource.Token(ctx)
	if err != nil {
		return nil, err
	}
	token.AuthHeader(req)
	return t.Base.RoundTrip(req)
}
