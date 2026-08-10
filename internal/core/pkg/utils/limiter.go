package utils

import (
	"context"
	"net/http"
)

// Limiter 리미터 인터페이스
type Limiter interface {
	Wait(ctx context.Context) error
}

type LimitTranport struct {
	Base    http.RoundTripper
	limiter Limiter
}

func (t *LimitTranport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if err := t.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return t.Base.RoundTrip(req)
}

func NewLimitClient(l Limiter, c *http.Client) *http.Client {
	t := &LimitTranport{
		Base:    c.Transport,
		limiter: l,
	}
	return &http.Client{
		Timeout:       c.Timeout,
		Transport:     t,
		CheckRedirect: c.CheckRedirect,
		Jar:           c.Jar,
	}
}
