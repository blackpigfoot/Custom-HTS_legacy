package kis

import (
	"Custom-HTS/internal/core/pkg/rate"
	"time"
)

const (
	realRPS  = 20 // 실전투자 초당 허용 요청 수
	paperRPS = 2  // 모의투자 초당 허용 요청 수

	// 토큰/승인키는 에러 루프 방지용. 실제 호출은 하루 1~2회.
	authIssueRPM = 1 // 분당 1회
)

func newRealLimiter() rate.Limiter {
	return rate.NewRateLimit(realRPS, realRPS)
}

func newPaperLimiter() rate.Limiter {
	return rate.NewRateLimit(paperRPS, paperRPS)
}

func newTokenLimiter() rate.Limiter {
	return rate.NewRateLimitPerInterval(authIssueRPM, time.Minute, authIssueRPM)
}

func newApprovalLimiter() rate.Limiter {
	return rate.NewRateLimitPerInterval(authIssueRPM, time.Minute, authIssueRPM)
}
