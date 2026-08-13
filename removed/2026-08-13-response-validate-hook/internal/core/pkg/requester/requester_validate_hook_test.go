// 원래 위치: internal/core/pkg/requester/{errors_test.go, requester_test.go}
// 제거 사유는 _legacy/README.md 참고.
//
// TestValidateRetryableRecovers가 겸하던 "재시도 간 ResultBody 초기화" 검증은
// requester_test.go의 TestResultBodyResetBetweenAttempts가 status 기반 재시도로 이어받았다.

package requester

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// ---- errors_test.go ----

// Retryable은 재시도 표시만 더하고 메시지와 errors.Is 동작은 원본을 따라야 한다
func TestRetryableIsTransparent(t *testing.T) {
	cause := errors.New("business error")
	marked := Retryable(cause)

	if marked.Error() != cause.Error() {
		t.Fatalf("메시지가 바뀜: %q", marked.Error())
	}
	if !errors.Is(marked, cause) {
		t.Fatal("원본 오류를 찾지 못함")
	}
	if !isRetryable(marked) {
		t.Fatal("재시도 표시가 인식되지 않음")
	}
	if isRetryable(cause) {
		t.Fatal("표시하지 않은 오류가 재시도 대상으로 인식됨")
	}
	if Retryable(nil) != nil {
		t.Fatal("nil을 감쌈")
	}

	// sentinel로 한 겹 더 감싸도 표시가 살아 있어야 한다
	if !isRetryable(fmt.Errorf("%w: %w", ErrResponseRejected, marked)) {
		t.Fatal("래핑 후 재시도 표시를 잃음")
	}
}

// ---- requester_test.go ----

// testResponse — rt_cd로 business error를 표현하는 테스트 응답 DTO
type testResponse struct {
	RtCd  string `json:"rt_cd"`
	MsgCd string `json:"msg_cd"`
}

func TestValidateRetryableRecovers(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"rt_cd":"1","msg_cd":"EXPIRED"}`))
			return
		}
		_, _ = w.Write([]byte(`{"rt_cd":"0"}`))
	}))
	defer srv.Close()

	var resp testResponse
	var recovered atomic.Int64
	r := newTestRequester(t, &Config{RetryPolicy: &RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}})

	err := r.Send(context.Background(), func(ctx context.Context) (*Item, error) {
		return &Item{
			Method:     http.MethodGet,
			Path:       srv.URL,
			ResultBody: &resp,
			Validate: func() error {
				if resp.RtCd == "0" {
					return nil
				}
				recovered.Add(1) // 어댑터의 복구 동작 자리 (토큰 폐기 등)
				return Retryable(errors.New("business error " + resp.MsgCd))
			},
		}, nil
	})
	if err != nil {
		t.Fatalf("재시도로 복구되지 않음: %v", err)
	}
	if hits.Load() != 2 || recovered.Load() != 1 {
		t.Fatalf("시도 %d회, 복구 %d회", hits.Load(), recovered.Load())
	}
	// 재시도 전에 대상이 초기화되어 첫 응답의 msg_cd가 남지 않아야 한다
	if resp.MsgCd != "" {
		t.Fatalf("이전 시도의 값이 남음: %+v", resp)
	}
}

func TestValidateNonRetryableFailsFast(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"rt_cd":"1","msg_cd":"FATAL"}`))
	}))
	defer srv.Close()

	sentinel := errors.New("fatal business error")
	var resp testResponse
	r := newTestRequester(t, &Config{RetryPolicy: &RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}})

	err := r.Send(context.Background(), func(ctx context.Context) (*Item, error) {
		return &Item{
			Method:     http.MethodGet,
			Path:       srv.URL,
			ResultBody: &resp,
			Validate:   func() error { return sentinel },
		}, nil
	})
	if hits.Load() != 1 {
		t.Fatalf("재시도 대상이 아닌데 다시 시도함: %d회", hits.Load())
	}
	if !errors.Is(err, ErrResponseRejected) || !errors.Is(err, sentinel) {
		t.Fatalf("오류 분류 실패: %v", err)
	}
}
