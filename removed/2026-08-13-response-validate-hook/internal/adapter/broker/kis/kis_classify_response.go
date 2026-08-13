// 원래 위치: internal/adapter/broker/kis/{rest.go, errors.go, rest_types.go}
// 제거 사유는 _legacy/README.md 참고.
//
// 현재는 rest.go의 handleKISError가 이 역할을 대신한다. 다만 재시도 표시는 하지 않고
// 토큰 폐기(복구)만 남긴 뒤 오류를 그대로 호출자에게 올린다.

package kis

import "Custom-HTS/internal/core/pkg/requester"

// ---- rest.go ----

// classifyKISError — KIS business error를 재시도 판정이 가능한 형태로 바꾸고,
// 다시 시도하기 전에 필요한 복구를 수행한다. 오류가 아니면 nil.
//
// KIS는 토큰 만료나 유량 초과를 HTTP status가 아니라 응답 body로 알려주므로 전송 계층만 보는
// requester는 판별할 수 없다. 벤더 지식이 필요한 판별과 복구는 여기서 맡고,
// 재시도 여부만 Retryable 표시로 requester에 넘긴다.
func (svc *REST) classifyKISError(kisErr *KISError) error {
	if kisErr == nil {
		return nil
	}

	switch {
	case isTokenExpired(kisErr.MsgCd):
		// 다음 시도의 GetToken이 새 토큰을 발급하도록 캐시와 토큰 파일을 폐기
		svc.auth.ClearToken()
		return requester.Retryable(kisErr)
	case isRateLimited(kisErr.MsgCd):
		return requester.Retryable(kisErr)
	}
	return kisErr
}

// ---- errors.go ----

// isRateLimited — 대기 후 재시도로 복구 가능한 KIS 오류인지 여부
func isRateLimited(msgCd string) bool {
	return msgCd == MsgCodeRateLimited
}

// ---- rest.go: sendREST 안. 재시도 루프 밖에서 판정 함수를 만들어 Item.Validate에 넘겼다 ----
//
//	// 판정 방법도 시도마다 달라지지 않으므로 한 번만 만든다.
//	// requester가 응답 body를 디코딩한 직후에 호출한다
//	validate := func() error { return svc.classifyKISError(req.resultChecker.CheckError()) }
//
//	... newAttempt 안의 requester.Item 리터럴 ...
//		Validate: validate,

// ---- rest_types.go: restRequest 필드. 지금은 sendREST의 세 번째 인자로 받는다 ----
//
//	// resultChecker — 디코딩된 응답에서 KIS business error를 꺼내는 주체.
//	//
//	// 원문 호출과 타입이 정해진 호출이 각자 자기 응답 타입으로 판정하므로
//	// sendREST는 응답 모양을 알 필요가 없다. 보통 resultBody와 같은 객체다
//	resultChecker responseChecker
