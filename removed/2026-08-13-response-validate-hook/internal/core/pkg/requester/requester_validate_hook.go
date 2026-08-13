// 원래 위치: internal/core/pkg/requester/{errors.go, types.go, requester.go}
// 제거 사유는 _legacy/README.md 참고.
//
// 되살릴 때는 아래 조각을 각 원본 파일의 표시된 자리로 되돌리면 된다.

package requester

import "errors"

// ---- errors.go: sentinel 목록에 추가 ----

// ErrResponseRejected — Item.Validate가 응답을 오류로 판정
var ErrResponseRejected = errors.New("response rejected")

// ---- errors.go: joinMessage 앞 ----

// retryableError — 호출자가 재시도 대상으로 표시한 오류.
//
// 메시지는 감싼 오류를 그대로 노출하고, 표시 자체는 shouldRetry만 인식한다.
type retryableError struct {
	// err — 재시도 대상으로 표시된 원본 오류
	err error
}

func (e *retryableError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *retryableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// Retryable — 호출자가 판별한 오류를 재시도 대상으로 표시.
//
// Item.Validate가 반환하면 requester가 남은 재시도 예산 안에서 다시 시도한다.
// 재시도 여부만 바꾸고 메시지와 errors.Is/As 동작은 원본 오류를 그대로 따른다.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{err: err}
}

// isRetryable — 호출자가 재시도 대상으로 표시한 오류인지 여부
func isRetryable(err error) bool {
	var marker *retryableError
	return errors.As(err, &marker)
}

// ---- types.go: Item.ResultBody와 Item.Release 사이 ----
//
//	// Validate — 디코딩 성공 후 호출하는 응답 검증 훅 (선택).
//	//
//	// HTTP status는 정상인데 응답 body가 벤더 오류를 담고 있는 경우를 판별한다.
//	// 반환한 오류는 재시도 판정 대상이 되며, 다시 시도하려면 Retryable로 감싸 반환한다.
//	// 토큰 폐기처럼 다음 시도 전에 끝내야 하는 복구 동작도 이 안에서 수행한다.
//	Validate func() error

// ---- requester.go: doRequest 끝. 현재는 `return decodeResponse(resp, item)` ----
//
//	if err := decodeResponse(resp, item); err != nil {
//		return err
//	}
//
//	if item.Validate != nil {
//		if err := item.Validate(); err != nil {
//			return fmt.Errorf("%w: %w", ErrResponseRejected, err)
//		}
//	}
//	return nil

// ---- requester.go: shouldRetry 첫 줄 ----
//
//	if isRetryable(err) {
//		return true
//	}

// ---- requester.go: 패키지 doc comment 원문 ----
//
// Package requester — HTTP 전송, 응답 디코딩, 공통 재시도 루프 담당.
//
// 재시도 판정 범위:
//   - 전송 계층: 네트워크 오류, timeout, 429, 5xx 등 requester가 단독으로 판단 가능한 일시 장애
//   - 응답 계층: HTTP status는 정상이나 body가 벤더 오류를 담은 경우. 판별은 requester가 하지 않고
//     Item.Validate가 Retryable로 표시한 오류만 재시도 대상으로 받는다
//
// 역할 분리: requester는 재시도 횟수와 backoff만 소유하고, 벤더 오류 판별과
// 토큰 폐기 같은 복구 동작은 브로커 어댑터가 Item.Validate 안에서 담당한다.
