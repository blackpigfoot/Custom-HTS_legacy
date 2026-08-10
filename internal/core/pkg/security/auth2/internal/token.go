package internal

import (
	"bytes"
	"context"
	"io"
	"net/http"
)

func RetriveToken(ctx context.Context, url string, h map[string]string, b []byte, c *http.Client) (http.Header, []byte, error) {
	req, err := newTokenRequst(url, h, b)
	if err != nil {
		return nil, nil, err
	}
	res, err := dotokenRoundTrip(ctx, req, c)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	rb, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, err
	}
	return res.Header, rb, nil
}

func newTokenRequst(url string, h map[string]string, b []byte) (*http.Request, error) {
	body := bytes.NewReader(b)
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
	return req, err
}

func dotokenRoundTrip(ctx context.Context, req *http.Request, c *http.Client) (*http.Response, error) {
	return c.Do(req.WithContext(ctx))
}
