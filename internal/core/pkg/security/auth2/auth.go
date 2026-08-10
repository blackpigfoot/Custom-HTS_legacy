package auth2

import (
	"context"
	"Custom-HTS/internal/core/pkg/security/auth2/internal"
	"net/http"
	"sync"
	"time"
)

// 긴급!!!!!!!!!!!!!!!!!
// 이제부터 이 패키지는 토큰 관리 자체의 기능으로 제한됩니다.
// 토큰 발급 방식과 토큰 갱신 방식이 다양하기 때문에
// 라운드트립으로 모두 처리할려면 너무 복잡해질 수 있습니다.
//

// 이 패키지는 oauth2 패키지의 컨텍스트 필드 저장 문제와
// OPENAPI 토큰 갱신 방식을 고려하여
// 별도의 인증 토큰 관리 패키지로 작성되었습니다.
//
// OPENAPI 토큰 발급 방식은 기본적으로 url.vaule 인코딩 방식이 아닌
// JSON 바디 전송 방식을 사용합니다.
// 또한 토큰 갱신 시 클라이언트 자격증명을 함께 전송해야 합니다.
//
// 향후 로그인 방식이 추가될 수 있습니다.
// -------------------------------------------------------

// 기본 만료 델타 (10분)
var Defaultexpiry = 10 * time.Minute

type Provider interface {
	GetHeader() map[string]string
	// 토큰 요청 바이트를 반환합니다.
	// 마샬링을 수행하거나 url.Values 인코딩을 수행하여
	// 바이트를 반환해야 합니다.
	GetBody() ([]byte, error)

	// 토큰 응답을 파싱하여 Token 구조체를 반환합니다.
	// 함수 내부에서 Body.Close를 호출하지 마시기 바랍니다.
	//
	// [Parse] 함수 호출자에서 Body.Close를 호출합니다.
	Parse(header http.Header, body []byte) (*Token, error)

	// 토큰 발급 요청 명세서에 클라이언트 자격증명을 설정합니다.
	//
	// 만약 사전에 세팅된 토큰 요청 구조체가 있다면
	// 내부적으로 업데이트 로직을 제거하거나 덮어씌우시길 바랍니다.
	SetCredential(*Config)
}

// Config 토큰 설정 구조체
type Config struct {
	AppKey    string
	AppSecret string
	// 토큰 발급 URL
	// TODO: 로그인 구현시 엔드포인트 구조체로 변경
	Authurl string
}

// Client 토큰 클라이언트 생성
func (c *Config) Client(t *Token, p Provider, h *http.Client) (*http.Client, error) {
	return NewClient(c.TokenSource(t, p, h), h), nil
}

// TokenSource 재사용 토큰 소스 생성
func (c *Config) TokenSource(t *Token, p Provider, h *http.Client) TokenSource {
	cl := internal.ClientOrDefault(h)
	tkr := &tokenReFresher{
		c:    cl,
		conf: c,
		p:    p,
	}
	return &reusetokensource{
		new:   tkr,
		token: t,
	}
}

// reusetokensource 토큰 발급을 뮤텍스로 감싸서 보호합니다.
// [TokenSource] 자체는 스레드 안전하지 않기 때문입니다.
type reusetokensource struct {
	// 토큰 발급시 호출
	new   TokenSource
	token *Token
	mu    sync.Mutex
}

func (r *reusetokensource) Token(ctx context.Context) (*Token, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.token.IsValid() {
		return r.token, nil
	}
	tok, err := r.new.Token(ctx)
	if err != nil {
		return nil, err
	}
	r.token = tok
	return tok, nil
}

type tokenReFresher struct {
	// 토큰 발급시 사용할 HTTP 클라이언트
	c    *http.Client
	conf *Config
	// 토큰 발급 방식
	p Provider
}

func (t *tokenReFresher) Token(ctx context.Context) (*Token, error) {
	t.p.SetCredential(t.conf)
	b, err := t.p.GetBody()
	if err != nil {
		return nil, err
	}
	h := t.p.GetHeader()
	rh, rb, err := internal.RetriveToken(ctx, t.conf.Authurl, h, b, t.c)
	if err != nil {
		return nil, err
	}
	return t.p.Parse(rh, rb)
}

// NewClient 토큰 소스와 리미터를 적용한 HTTP 클라이언트를 생성합니다.
//
// 클라이언트가 nil이면 기본 HTTP 클라이언트를 사용합니다.
// 반환된 클라이언트는 토큰 소스를 사용하여 요청 시마다
// 토큰 라운드트립을 수행합니다.
//
// 만약 재사용할 토큰이 있는 경우
// [ReuseTokenSource]를 사용하여 토큰 소스를 래핑한 후
// 이 함수에 전달해야 합니다.
func NewClient(src TokenSource, h *http.Client) *http.Client {

	if src == nil {
		return internal.ClientOrDefault(h)
	}
	cl := internal.ClientOrDefault(h)

	var t http.RoundTripper
	t = &Transport{
		Base:        cl.Transport,
		TokenSource: ReuseTokenSource(nil, src),
	}
	return &http.Client{
		Timeout:       cl.Timeout,
		Transport:     t,
		CheckRedirect: cl.CheckRedirect,
		Jar:           cl.Jar,
	}
}

// ReuseTokenSource 뮤텍스로 tokenReFresher를 래핑하는 TokenSource를 반환합니다.
// 기존 토큰이 유효하면 재사용합니다.
// 만약 src가 이미 reusetoken 타입이면 래핑하지 않습니다.
//
// 만약 [NewClient]를 바로 호출할 예정이며 재사용할 Token이 있는 경우
// [NewClient]는 Token을 인자로 받지 않기 때문에
// [ReuseTokenSource를] 사용하여 Token을 재사용할 수 있도록 해야 합니다.
//
// 이 함수로 래핑후 [NewClient]에 전달하면 캐싱된 토큰을 사용 가능합니다.
func ReuseTokenSource(t *Token, src TokenSource) TokenSource {
	if rt, ok := src.(*reusetokensource); ok {
		if t == nil {
			// Just use it directly.
			return rt
		}
		src = rt.new
	}
	return &reusetokensource{
		token: t,
		new:   src,
	}
}
