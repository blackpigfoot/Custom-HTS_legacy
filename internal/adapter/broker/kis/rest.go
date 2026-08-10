package kis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"Custom-HTS/internal/core/exchange"
	"Custom-HTS/internal/core/pkg/requester"
)

// sendREST — 모든 KIS REST 호출의 공통 진입점.
//
// 역할:
//  1. tokenStore.Get()으로 토큰 획득
//  2. KIS 필수 헤더(authorization, appkey, appsecret, tr_id) 주입
//  3. Requester.SendPayload()로 전송
//
// 모든 메서드가 이 함수를 통하므로 헤더 누락/중복 없음.
// path는 호출부에서 완성된 URL을 넘긴다 (BaseURL + path + query params).
func (b *Broker) sendREST(ctx context.Context, req restRequest) error {
	err := b.SendPayload(ctx, b.limiter, func() (*requester.Item, error) {
		token, err := b.tokenStore.Get(ctx)
		if err != nil {
			return nil, err
		}

		var body io.Reader
		if req.bodyReq != nil {
			bs, err := json.Marshal(req.bodyReq)
			if err != nil {
				return nil, fmt.Errorf("marshaling request body: %w", err)
			}
			body = bytes.NewReader(bs)
		}

		return &requester.Item{
			Method: req.method,
			Path:   req.path,
			Headers: map[string]string{
				"authorization": "Bearer " + token,
				"appkey":        b.appKey,
				"appsecret":     b.appSecret,
				"tr_id":         req.trID,
				"Content-Type":  "application/json; charset=utf-8",
			},
			Body:   body,
			Result: req.result,
		}, nil
	})

	if err != nil {
		return err
	}

	if checker, ok := req.result.(ResponseChecker); ok {
		if apiErr := checker.CheckError(); apiErr != nil {
			return apiErr
		}
	}

	return nil
}

// GetPrice — 현재가 조회.
func (b *Broker) GetPrice(ctx context.Context, code string) (*exchange.Price, error) {
	reqURL, err := buildURL(b.BaseURL, PathPrice, map[string]string{
		"FID_COND_MRKT_DIV_CODE": "J",
		"FID_INPUT_ISCD":         code,
	})
	if err != nil {
		return nil, err
	}

	var resp priceResponse
	if err := b.sendREST(ctx, restRequest{
		method: http.MethodGet,
		path:   reqURL,
		trID:   TrIDPrice,
		result: &resp,
	}); err != nil {
		return nil, fmt.Errorf("GetPrice(%s): %w", code, err)
	}

	return parsePrice(code, resp)
}

// GetOrderbook — Phase 2에서 구현.
func (b *Broker) GetOrderbook(_ context.Context, _ string) (*exchange.Orderbook, error) {
	return nil, fmt.Errorf("GetOrderbook: not implemented yet")
}

// GetCandles — Phase 5에서 구현.
func (b *Broker) GetCandles(_ context.Context, _ string, _ string, _ int) ([]exchange.Candle, error) {
	return nil, fmt.Errorf("GetCandles: not implemented yet")
}

// PlaceOrder — 현금 매수/매도 주문.
func (b *Broker) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (*exchange.OrderResponse, error) {
	ordDvsn := "00"
	ordUnpr := strconv.FormatInt(req.Price, 10)
	ordQty := strconv.FormatInt(req.Quantity, 10)

	if req.Type == exchange.Market {
		ordDvsn = "01"
		ordUnpr = "0"
	}

	var resp orderResponse
	if err := b.sendREST(ctx, restRequest{
		method: http.MethodPost,
		path:   b.BaseURL + PathOrderCash,
		trID:   b.orderTrID(req.Side),
		bodyReq: orderRequest{
			CANO:    b.accountNo,
			ACNT:    b.accountSub,
			PDNO:    req.Code,
			OrdDvSn: ordDvsn,
			OrdQty:  ordQty,
			OrdUnpr: ordUnpr,
		},
		result: &resp,
	}); err != nil {
		return nil, fmt.Errorf("PlaceOrder: %w", err)
	}

	return &exchange.OrderResponse{
		OrderID:   resp.Output.OrdNo,
		Code:      req.Code,
		Side:      req.Side,
		Type:      req.Type,
		Quantity:  req.Quantity,
		Price:     req.Price,
		Status:    exchange.StatusOpen,
		CreatedAt: time.Now(),
	}, nil
}

// CancelOrder — Phase 3에서 구현.
func (b *Broker) CancelOrder(_ context.Context, _ string) error {
	return fmt.Errorf("CancelOrder: not implemented yet")
}

// GetOpenOrders — Phase 3에서 구현.
func (b *Broker) GetOpenOrders(_ context.Context) ([]exchange.Order, error) {
	return nil, fmt.Errorf("GetOpenOrders: not implemented yet")
}

// GetOrderHistory — Phase 3에서 구현.
func (b *Broker) GetOrderHistory(_ context.Context, _ exchange.HistoryOpts) ([]exchange.Order, error) {
	return nil, fmt.Errorf("GetOrderHistory: not implemented yet")
}

// GetBalance — 잔고 조회.
func (b *Broker) GetBalance(ctx context.Context) (*exchange.Balance, error) {
	trID := TrIDBalanceReal
	if b.IsPaper {
		trID = TrIDBalancePaper
	}

	reqURL, err := buildURL(b.BaseURL, PathBalance, map[string]string{
		"CANO":                  b.accountNo,
		"ACNT_PRDT_CD":          b.accountSub,
		"AFHR_FLPR_YN":          "N",
		"OFL_YN":                "",
		"INQR_DVSN":             "02",
		"UNPR_DVSN":             "01",
		"FUND_STTL_ICLD_YN":     "N",
		"FNCG_AMT_AUTO_RDPT_YN": "N",
		"PRCS_DVSN":             "01",
		"CTX_AREA_FK100":        "",
		"CTX_AREA_NK100":        "",
	})
	if err != nil {
		return nil, err
	}

	var resp balanceResponse
	if err := b.sendREST(ctx, restRequest{
		method: http.MethodGet,
		path:   reqURL,
		trID:   trID,
		result: &resp,
	}); err != nil {
		return nil, fmt.Errorf("GetBalance: %w", err)
	}

	balance := &exchange.Balance{}
	for _, h := range resp.Output1 {
		holding, err := parseHolding(h)
		if err != nil {
			return nil, fmt.Errorf("parsing holding %s: %w", h.Pdno, err)
		}
		balance.Holdings = append(balance.Holdings, holding)
	}

	if len(resp.Output2) > 0 {
		s := resp.Output2[0]
		ta, err := parseKRWInt(s.TotEvluAmt)
		if err != nil {
			return nil, fmt.Errorf("parsing total asset: %w", err)
		}
		cb, err := parseKRWInt(s.DncaTotAmt)
		if err != nil {
			return nil, fmt.Errorf("parsing cash balance: %w", err)
		}
		balance.TotalAsset = ta
		balance.CashBalance = cb
	}
	return balance, nil
}

// parseAccountNo — "50071022-01" 형식 분리.
func (b *Broker) parseAccountNo(accountNo string) error {
	parts := strings.SplitN(accountNo, "-", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid account number format: %q (expected XXXXXXXX-XX)", accountNo)
	}
	b.accountNo = parts[0]
	b.accountSub = parts[1]
	return nil
}

// orderTrID — 매수/매도 + 실전/모의 tr_id 반환.
func (b *Broker) orderTrID(side exchange.Side) string {
	if b.IsPaper {
		if side == exchange.Buy {
			return TrIDBuyPaper
		}
		return TrIDSellPaper
	}
	if side == exchange.Buy {
		return TrIDBuyReal
	}
	return TrIDSellReal
}

// parseKRWInt — KRW 금액/가격 문자열 → int64.
//
// KIS 가격 응답은 정수 문자열("72000")이 대부분이나,
// 매입평균가(pchs_avg_pric) 등 일부 필드에 소수점이 올 수 있어
// "71523.50" 같은 입력도 반올림으로 처리한다.
// float64 파싱은 경계 변환 1회뿐이므로 누적 오차 없음.
func parseKRWInt(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if strings.ContainsRune(s, '.') {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, err
		}
		return int64(math.Round(f)), nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// parseRate — 비율 문자열("1.25") → int64 고정소수점 (×RateScale).
//
// 1.25% → 12500, -0.03% → -300.
// float64 파싱은 경계 변환 1회뿐이므로 누적 오차 없음.
func parseRate(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return int64(math.Round(f * float64(RateScale))), nil
}

func parsePrice(code string, resp priceResponse) (*exchange.Price, error) {
	current, err := parseKRWInt(resp.Output.StckPrpr)
	if err != nil {
		return nil, fmt.Errorf("parsing current price: %w", err)
	}
	open, err := parseKRWInt(resp.Output.StckOprc)
	if err != nil {
		return nil, fmt.Errorf("parsing open: %w", err)
	}
	high, err := parseKRWInt(resp.Output.StckHgpr)
	if err != nil {
		return nil, fmt.Errorf("parsing high: %w", err)
	}
	low, err := parseKRWInt(resp.Output.StckLwpr)
	if err != nil {
		return nil, fmt.Errorf("parsing low: %w", err)
	}
	prevClose, err := parseKRWInt(resp.Output.StckSdpr)
	if err != nil {
		return nil, fmt.Errorf("parsing prev close: %w", err)
	}
	volume, err := parseInt64(resp.Output.AcmlVol)
	if err != nil {
		return nil, fmt.Errorf("parsing volume: %w", err)
	}
	change, err := parseKRWInt(resp.Output.PrdyVrss)
	if err != nil {
		return nil, fmt.Errorf("parsing change: %w", err)
	}
	changePct, err := parseRate(resp.Output.PrdyCtrt)
	if err != nil {
		return nil, fmt.Errorf("parsing change pct: %w", err)
	}

	return &exchange.Price{
		Code: code, Current: current, Open: open, High: high,
		Low: low, PrevClose: prevClose, Volume: volume,
		Change: change, ChangePct: changePct, Timestamp: time.Now(),
	}, nil
}

func parseHolding(h balanceHolding) (exchange.Holding, error) {
	qty, err := parseInt64(h.HldgQty)
	if err != nil {
		return exchange.Holding{}, fmt.Errorf("quantity: %w", err)
	}
	avgCost, err := parseKRWInt(h.PchsAvgPric)
	if err != nil {
		return exchange.Holding{}, fmt.Errorf("avg cost: %w", err)
	}
	curPrice, err := parseKRWInt(h.Prpr)
	if err != nil {
		return exchange.Holding{}, fmt.Errorf("current price: %w", err)
	}
	pnl, err := parseKRWInt(h.EvluPflsAmt)
	if err != nil {
		return exchange.Holding{}, fmt.Errorf("pnl: %w", err)
	}
	pnlPct, err := parseRate(h.EvluPflsRt)
	if err != nil {
		return exchange.Holding{}, fmt.Errorf("pnl pct: %w", err)
	}

	return exchange.Holding{
		Code: h.Pdno, Name: h.PrdtNm, Quantity: qty,
		AvgCost: avgCost, CurrentPrice: curPrice, PnL: pnl, PnLPct: pnlPct,
	}, nil
}

// parseInt64 — 정수 문자열 파싱. 빈 문자열은 0 반환.
// ws_parser.go에서도 동일 함수를 사용하므로 패키지 내 공용.
func parseInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// KISError — KIS API 논리적 에러 처리를 위한 커스텀 에러.
type KISError struct {
	RtCd  string
	MsgCd string
	Msg   string
}

func (e *KISError) Error() string {
	return fmt.Sprintf("kis api error [%s]: %s (rt_cd: %s)", e.MsgCd, e.Msg, e.RtCd)
}

// ResponseChecker — KIS 응답 구조체가 에러 검증 로직을 구현하도록 강제하는 인터페이스.
// kisResponse.CheckError()로 구현됨 (rest_types.go).
type ResponseChecker interface {
	CheckError() error
}

// buildURL — net/url 패키지를 사용한 안전한 URL 및 쿼리 파라미터 조립 헬퍼.
func buildURL(baseURL, path string, params map[string]string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid base url: %w", err)
	}
	u.Path = path

	if len(params) > 0 {
		q := u.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}
