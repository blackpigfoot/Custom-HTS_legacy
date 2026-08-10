package subscription

// SubType — 구독 종류.
type SubType string

const (
	SubTypeTrade SubType = "trade"
	SubTypeQuote SubType = "quote"
	SubTypeExec  SubType = "exec"
)

// Sub — 구독 단위 식별자.
//
// map key로 사용하므로 comparable해야 함.
// Code + Tr 조합이 구독의 고유 식별자.
// 거래소 정보는 포함하지 않음 — 어느 broker에 할당할지는 Manager.info가 관리.
type Sub struct {
	Code string  // "005930", "KRW-BTC"
	Tr   SubType // trade, quote, exec
}

func (s Sub) String() string {
	return string(s.Tr) + ":" + s.Code
}
