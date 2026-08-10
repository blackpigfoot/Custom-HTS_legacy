package kis

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"Custom-HTS/internal/core/exchange"
	"Custom-HTS/internal/core/pkg/logger"
	"Custom-HTS/internal/core/pkg/requester"
	"Custom-HTS/internal/core/pkg/security/keystore"
)

// ─── 분봉 차트 API (FHKST03010230) ──────────────────────────────────────────

const (
	// PathMinuteChart — 주식당일분봉조회 endpoint.
	// TR에 따라 당일(03010200) 또는 기간별(03010230) 분봉 조회.
	PathMinuteChart = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
	TrIDMinuteChart = "FHKST03010230"
)

// MinuteCandle — 파싱된 1분봉 데이터.
type MinuteCandle struct {
	Date    string  // YYYYMMDD
	Time    string  // HHMMSS
	Close   int64   // 현재가(종가)
	Open    int64   // 시가
	High    int64   // 고가
	Low     int64   // 저가
	Volume  int64   // 체결거래량
	ChgRate float64 // 전일대비율(%) — 전략 조건 판단에 직접 사용
}

// minuteChartResponse — FHKST03010230 응답 구조.
type minuteChartResponse struct {
	kisResponse
	Output1 minuteChartMeta   `json:"output1"`
	Output2 []minuteChartItem `json:"output2"`
}

type minuteChartMeta struct {
	StckPrpr     string `json:"stck_prpr"`      // 주식현재가
	PrdyVrss     string `json:"prdy_vrss"`      // 전일대비
	PrdyCtrt     string `json:"prdy_ctrt"`      // 전일대비율
	StckPrdyClpr string `json:"stck_prdy_clpr"` // 전일종가
}

type minuteChartItem struct {
	StckBsopDate string `json:"stck_bsop_date"` // 주식영업일자 YYYYMMDD
	StckCntgHour string `json:"stck_cntg_hour"` // 주식체결시간 HHMMSS
	StckPrpr     string `json:"stck_prpr"`       // 주식현재가
	StckOprc     string `json:"stck_oprc"`       // 주식시가
	StckHgpr     string `json:"stck_hgpr"`       // 주식최고가
	StckLwpr     string `json:"stck_lwpr"`       // 주식최저가
	CntgVol      string `json:"cntg_vol"`        // 체결거래량
	AcmlVol      string `json:"acml_vol"`        // 누적거래량
	PrdyVrss     string `json:"prdy_vrss"`       // 전일대비
	PrdyVrssSign string `json:"prdy_vrss_sign"`  // 전일대비부호
	PrdyCtrt     string `json:"prdy_ctrt"`       // 전일대비율(%)
}

// GetMinuteChart — 특정 날짜의 1분봉 전체 조회.
//
// FHKST03010230 API는 지정 시각부터 과거 방향으로 최대 120건을 반환.
// endTime부터 역순으로 페이지네이션하여 09:00까지 전체 수집 후 시간순 정렬.
//
// 비영업일(주말/공휴일) 요청 시 API가 직전 영업일 데이터를 반환하므로,
// 응답의 stck_bsop_date를 확인하여 요청 날짜와 다르면 빈 결과로 조기 종료.
//
//	code: 종목코드 (e.g. "005930")
//	date: 조회일자 YYYYMMDD (e.g. "20260403")
func (b *Broker) GetMinuteChart(ctx context.Context, code, date string) ([]MinuteCandle, error) {
	var all []MinuteCandle
	var prevClose int64 // 전일종가 — output1에서 1회만 파싱
	curTime := "153000" // 장 마감부터 역순 조회
	seen := make(map[string]struct{}) // 중복 방지: "HHMMSS" 키

	for {
		reqURL, err := buildURL(b.BaseURL, PathMinuteChart, map[string]string{
			"FID_COND_MRKT_DIV_CODE": "J",
			"FID_INPUT_ISCD":         code,
			"FID_INPUT_DATE_1":       date,
			"FID_INPUT_HOUR_1":       curTime,
			"FID_PW_DATA_INCU_YN":    "N",
			"FID_FAKE_TICK_INCU_YN" : " ",
		})
		if err != nil {
			return nil, err
		}

		var resp minuteChartResponse
		if err := b.sendREST(ctx, restRequest{
			method: http.MethodGet,
			path:   reqURL,
			trID:   TrIDMinuteChart,
			result: &resp,
		}); err != nil {
			return nil, fmt.Errorf("GetMinuteChart(%s, %s): %w", code, date, err)
		}

		if len(resp.Output2) == 0 {
			break
		}

		// 첫 페이지에서 전일종가 파싱 (1회만)
		if prevClose == 0 && resp.Output1.StckPrdyClpr != "" {
			prevClose, _ = parseKRWInt(resp.Output1.StckPrdyClpr)
		}

		// 첫 응답의 날짜가 요청 날짜와 다르면 비영업일 → 즉시 종료
		if resp.Output2[0].StckBsopDate != date {
			break
		}

		matched := 0
		for _, item := range resp.Output2 {
			if item.StckBsopDate != date {
				continue
			}
			if _, dup := seen[item.StckCntgHour]; dup {
				continue
			}
			seen[item.StckCntgHour] = struct{}{}

			candle, err := parseMinuteCandle(item)
			if err != nil {
				return nil, fmt.Errorf("parsing candle %s %s: %w", code, item.StckCntgHour, err)
			}
			all = append(all, candle)
			matched++
		}

		// 120건 미만이면 마지막 페이지
		if len(resp.Output2) < 120 {
			break
		}

		// 이 페이지에서 요청 날짜 데이터가 하나도 없으면 종료
		if matched == 0 {
			break
		}

		// 마지막 항목의 시각 → 다음 페이지 조회 기준
		last := resp.Output2[len(resp.Output2)-1]
		if last.StckCntgHour <= "090000" || last.StckCntgHour >= curTime {
			break
		}
		curTime = last.StckCntgHour

		time.Sleep(200 * time.Millisecond) // API rate limit 준수
	}

	// 역순으로 수집했으므로 시간순 정렬
	reverseCandles(all)

	// 전일종가 기반 전일대비율 계산 (output2의 prdy_ctrt는 현재가 기준이라 백테스트 부적합)
	if prevClose > 0 {
		for i := range all {
			all[i].ChgRate = float64(all[i].Close-prevClose) / float64(prevClose) * 100
		}
	}

	return all, nil
}

func parseMinuteCandle(item minuteChartItem) (MinuteCandle, error) {
	cl, err := parseKRWInt(item.StckPrpr)
	if err != nil {
		return MinuteCandle{}, fmt.Errorf("close: %w", err)
	}
	op, err := parseKRWInt(item.StckOprc)
	if err != nil {
		return MinuteCandle{}, fmt.Errorf("open: %w", err)
	}
	hi, err := parseKRWInt(item.StckHgpr)
	if err != nil {
		return MinuteCandle{}, fmt.Errorf("high: %w", err)
	}
	lo, err := parseKRWInt(item.StckLwpr)
	if err != nil {
		return MinuteCandle{}, fmt.Errorf("low: %w", err)
	}
	vol, err := parseInt64(item.CntgVol)
	if err != nil {
		return MinuteCandle{}, fmt.Errorf("volume: %w", err)
	}
	chgRate, _ := strconv.ParseFloat(strings.TrimSpace(item.PrdyCtrt), 64)

	return MinuteCandle{
		Date:    item.StckBsopDate,
		Time:    item.StckCntgHour,
		Close:   cl,
		Open:    op,
		High:    hi,
		Low:     lo,
		Volume:  vol,
		ChgRate: chgRate,
	}, nil
}

func reverseCandles(s []MinuteCandle) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// ─── 경량 생성자 ─────────────────────────────────────────────────────────────

// NewLite — REST 전용 경량 Broker.
//
// 백테스트, 데이터 수집 등 REST API만 필요한 용도.
// WS, 서킷브레이커, BrokerChannels 없이 토큰 + REST만 초기화.
func NewLite(appKey, appSecret string, isPaper bool) (*Broker, error) {
	baseURL := BaseURLReal
	if isPaper {
		baseURL = BaseURLPaper
	}

	req, err := requester.New(&requester.Config{Name: "kis-lite"})
	if err != nil {
		return nil, fmt.Errorf("creating requester: %w", err)
	}

	b := &Broker{
		Base: exchange.Base{
			BaseURL:   baseURL,
			IsPaper:   isPaper,
			Requester: req,
			Log:       logger.Default(),
		},
		appKey:    appKey,
		appSecret: appSecret,
	}

	if isPaper {
		b.limiter = newPaperLimiter()
	} else {
		b.limiter = newRealLimiter()
	}
	b.tokenLimiter = newTokenLimiter()
	b.tokenStore = keystore.NewKeyStore(5*time.Minute, b.issueToken)

	return b, nil
}
