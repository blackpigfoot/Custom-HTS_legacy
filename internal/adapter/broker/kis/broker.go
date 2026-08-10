package kis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"Custom-HTS/internal/core/domain"
	"Custom-HTS/internal/core/exchange"
	"Custom-HTS/internal/core/pkg/circuitbreaker"
	"Custom-HTS/internal/core/pkg/rate"
	"Custom-HTS/internal/core/pkg/requester"
	"Custom-HTS/internal/core/pkg/security/keystore"
	ws "Custom-HTS/internal/core/pkg/websocket"
	"Custom-HTS/internal/core/service/config"
)

// Broker — KIS Exchange 인터페이스 구현체.
type Broker struct {
	exchange.Base

	// 인증 / 설정
	appKey    string
	appSecret string

	// 계좌
	accountNo  string // "50071022" (앞 8자리)
	accountSub string // "01" (뒤 2자리)

	limiter         rate.Limiter
	tokenLimiter    rate.Limiter
	approvalLimiter rate.Limiter

	// 키 저장소
	tokenStore    *keystore.KeyStore
	approvalStore *keystore.KeyStore

	// WebSocket
	conn        *ws.WsConnection
	encKey      string
	encIV       string
	wsStartOnce sync.Once

	// 서킷브레이커 — 메시지 타입별 연속 실패 감지.
	// 파싱 실패가 임계값을 넘으면 EventBus로 에러 발행.
	// AccountManager가 TopicError를 수신하여 후속 처리.
	tradeBreaker   *circuitbreaker.Breaker
	quoteBreaker   *circuitbreaker.Breaker
	execBreaker    *circuitbreaker.Breaker
	decryptBreaker *circuitbreaker.Breaker
}

// New — KIS Broker 생성.
func New(p *exchange.SetupParams) (exchange.Exchange, error) {
	log := p.Log.With("broker", p.Broker.String(), "account", p.ID)

	b := &Broker{
		Base: exchange.Base{
			AccountID: p.ID,
			Broker:    p.Broker.String(),
			BaseURL:   resolveBaseURL(p.IsPaper, p.BaseURL),
			IsPaper:   p.IsPaper,
			AssetType: config.AssetStock,
			Config:    p.AccountConfig,
			Log:       log,
		},
		appKey:    p.Credentials.APIKey,
		appSecret: p.Credentials.APISecret,
	}

	if err := b.parseAccountNo(p.Credentials.AccountNo); err != nil {
		return nil, err
	}
	b.setLimiters()

	req, err := requester.New(&requester.Config{
		Name: p.Broker.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("creating requester: %w", err)
	}
	b.Requester = req

	b.tokenStore = keystore.NewKeyStore(5*time.Minute, b.issueToken)
	b.approvalStore = keystore.NewKeyStore(10*time.Minute, b.issueApprovalKey)

	b.initBreakers()

	wsURL := wsURLReal
	if p.IsPaper {
		wsURL = wsURLPaper
	}
	conn, err := ws.New(ws.WsConfig{
		URL:          wsURL,
		ReconnectMin: 1 * time.Second,
		ReconnectMax: 60 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("creating websocket connection: %w", err)
	}
	b.conn = conn

	return b, nil
}

// initBreakers — 메시지 타입별 서킷브레이커 초기화.
//
// 임계값 10회: 파싱 구조가 바뀌거나 데이터 손상이 지속되는 수준.
// onOpen: EventBus TopicError 발행. AccountManager가 수신하여 후속 처리.
func (b *Broker) initBreakers() {
	makeOnOpen := func(code, msg string) func(int64) {
		return func(count int64) {
			b.Log.Warn("circuit breaker opened",
				"code", code,
				"count", count,
			)
			b.Channels.SendError(domain.ErrorEvent{
				AccountID:  b.AccountID,
				Broker:     b.Broker,
				Source:     "kis_ws",
				Code:       code,
				Message:    msg,
				ReceivedAt: time.Now(),
			})
		}
	}

	makeOnClose := func(code string) func() {
		return func() {
			b.Log.Info("circuit breaker closed", "code", code)
		}
	}

	b.tradeBreaker = circuitbreaker.New(10,
		makeOnOpen("TRADE_PARSE_FAIL", "체결 데이터 연속 파싱 실패"),
		makeOnClose("TRADE_PARSE_FAIL"),
	)
	b.quoteBreaker = circuitbreaker.New(10,
		makeOnOpen("QUOTE_PARSE_FAIL", "호가 데이터 연속 파싱 실패"),
		makeOnClose("QUOTE_PARSE_FAIL"),
	)
	b.execBreaker = circuitbreaker.New(10,
		makeOnOpen("EXEC_PARSE_FAIL", "체결통보 파싱 연속 실패"),
		makeOnClose("EXEC_PARSE_FAIL"),
	)
	b.decryptBreaker = circuitbreaker.New(3,
		makeOnOpen("EXEC_DECRYPT_FAIL", "체결통보 복호화 연속 실패"),
		makeOnClose("EXEC_DECRYPT_FAIL"),
	)
}

// Start — WS 연결 시작 (비동기).
// 폴링은 AccountManager가 조율하므로 Broker에서는 WS만 시작.
func (b *Broker) Start(ctx context.Context) {
	b.wsStartOnce.Do(func() {
		go b.runTransportMessages(ctx)
		go b.runTransportStatus(ctx)
		go b.conn.Run(ctx)
	})
}

// Conn — WsConnection 접근. 테스트, 외부 콜백 등록 시 사용.
func (b *Broker) Conn() *ws.WsConnection {
	return b.conn
}

func resolveBaseURL(isPaper bool, cfgBaseURL string) string {
	if cfgBaseURL != "" {
		return cfgBaseURL
	}
	if isPaper {
		return BaseURLPaper
	}
	return BaseURLReal
}

func (b *Broker) setLimiters() {
	if b.IsPaper {
		b.limiter = newPaperLimiter()
	} else {
		b.limiter = newRealLimiter()
	}
	b.tokenLimiter = newTokenLimiter()
	b.approvalLimiter = newApprovalLimiter()
}
