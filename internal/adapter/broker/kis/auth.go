package kis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"Custom-HTS/internal/core/pkg/requester"
	"Custom-HTS/internal/core/pkg/security/keystore"
)

// issueToken — 액세스 토큰 발급. tokenStore의 issue 함수로 주입됨.
func (b *Broker) issueToken(ctx context.Context) (*keystore.KeyInfo, error) {
	body, err := json.Marshal(tokenRequest{
		GrantType: "client_credentials",
		AppKey:    b.appKey,
		AppSecret: b.appSecret,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling token request: %w", err)
	}

	var resp tokenResponse
	if err := b.SendPayload(ctx, b.tokenLimiter, func() (*requester.Item, error) {
		return &requester.Item{
			Method:  http.MethodPost,
			Path:    b.BaseURL + PathTokenIssue,
			Headers: map[string]string{"Content-Type": "application/json"},
			Body:    bytes.NewReader(body),
			Result:  &resp,
		}, nil
	}); err != nil {
		return nil, fmt.Errorf("issuing token: %w", err)
	}

	if !resp.isSuccess() {
		return nil, fmt.Errorf("empty access token in response")
	}
	return &keystore.KeyInfo{
		Value:     resp.AccessToken,
		ExpiresAt: time.Now().Add(time.Duration(resp.ExpiresIn)*time.Second - tokenExpiryMargin),
	}, nil
}

// issueApprovalKey — WS 승인키 발급. approvalStore의 issue 함수로 주입됨.
//
// approvalKeyRequest의 AppSecret 필드명이 "secretkey"로 토큰과 다름에 주의.
func (b *Broker) issueApprovalKey(ctx context.Context) (*keystore.KeyInfo, error) {
	body, err := json.Marshal(approvalKeyRequest{
		GrantType: "client_credentials",
		AppKey:    b.appKey,
		AppSecret: b.appSecret,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling approval key request: %w", err)
	}

	var resp approvalKeyResponse
	if err := b.SendPayload(ctx, b.approvalLimiter, func() (*requester.Item, error) {
		return &requester.Item{
			Method:  http.MethodPost,
			Path:    b.BaseURL + PathApprovalKey,
			Headers: map[string]string{"Content-Type": "application/json; utf-8"},
			Body:    bytes.NewReader(body),
			Result:  &resp,
		}, nil
	}); err != nil {
		return nil, fmt.Errorf("issuing approval key: %w", err)
	}

	if !resp.isSuccess() {
		return nil, fmt.Errorf("empty approval key in response")
	}
	return &keystore.KeyInfo{
		Value:     resp.ApprovalKey,
		ExpiresAt: time.Now().Add(ApprovalKeyValidity - tokenExpiryMargin),
	}, nil
}
