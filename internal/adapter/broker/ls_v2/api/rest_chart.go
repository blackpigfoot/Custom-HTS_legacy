package api

import (
	"context"
	"errors"
	"Custom-HTS/internal/core/pkg/requester"
	"strings"
	"time"
)

var (
	InVaildPageTimeout = errors.New("invalid page timeout: must be positive")
)

// ChartMinute fetches t8412 minute candles, following continuation pages when
// req.Cont is true.
//
// Context usage:
//   - Pass a parent ctx WITHOUT a deadline (e.g. context.Background or a
//     cancel-only ctx from signal.NotifyContext). Each page derives its own
//     per-request timeout from req.PerRequestTimeout, so a deadline on the
//     parent would be consumed across all pages and fail on long continuations.
//   - The parent ctx still controls cancellation: cancelling it (or Ctrl+C via
//     NotifyContext) propagates to the in-flight page and aborts immediately.
//   - The caller does NOT need to know how many continuation pages a date range
//     will produce. Per-request timeout protects each page independently; total
//     duration is unbounded by design and bounded only by parent cancellation.
//
// T8412Response time example:
//
//	[
//	  {"time": "090100"},
//	  {"time": "090200"}
//	]
func (svc *REST) ChartMinute(ctx context.Context, req ChartMinuteRequest) (requester.Header, *T8412Response, error) {
	var merged *T8412Response
	nextTrCont := "N"
	nextTrContKey := ""
	nextCtsDate := " "
	nextCtsTime := " "
	var lastHeader requester.Header
	if req.PageTimeout <= 0 {
		return nil, nil, InVaildPageTimeout
	}
	for {
		resp, header, err := svc.chartMinutePage(
			ctx, req.PageTimeout,
			req, nextTrCont, nextTrContKey, nextCtsDate, nextCtsTime,
		)
		if err != nil {
			return nil, nil, &OperationError{Op: "ls get chart minute", Err: err}
		}
		lastHeader = header

		if merged == nil {
			merged = resp
		} else {
			// OutBlock은 마지막 페이지 기준 갱신 (메타·연속키 최신화).
			merged.T8412OutBlock = resp.T8412OutBlock
			// OutBlock1(분봉 리스트)은 누적.
			merged.T8412OutBlock1 = append(merged.T8412OutBlock1, resp.T8412OutBlock1...)
		}

		nextTrCont = strings.TrimSpace(header.Get("tr_cont"))
		nextTrContKey = strings.TrimSpace(header.Get("tr_cont_key"))
		nextCtsDate = strings.TrimSpace(resp.T8412OutBlock.Cts_Date)
		nextCtsTime = strings.TrimSpace(resp.T8412OutBlock.Cts_Time)

		// 종료 조건: 연속조회 비활성 OR 헤더가 "Y" 아님 OR 연속키 소진
		if !req.Cont || !strings.EqualFold(nextTrCont, "Y") || nextCtsTime == "" {
			return lastHeader, merged, nil
		}
	}
}

// chartMinutePage sends a single t8412 page request.
//
// The per-request timeout is derived from the parent ctx via WithTimeout, so
// each page gets a fresh deadline while still propagating parent cancellation.
// cancel runs on return (function scope), preventing the context leak that a
// loop-level defer would cause.
func (svc *REST) chartMinutePage(
	ctx context.Context, timeout time.Duration,
	req ChartMinuteRequest,
	trCont, trContKey, ctsDate, ctsTime string,
) (*T8412Response, requester.Header, error) {
	pageCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var resp T8412Response
	header := make(requester.Header)
	if err := svc.sendREST(pageCtx, restRequest{
		method: requester.PostMethod,
		path:   PathStockChart,
		trID:   TrIDChartMinute,
		bodyReq: t8412Request{
			In: t8412InBlock{
				Shcode:   req.Shcode,
				Ncnt:     req.Minute,
				Qrycnt:   req.Qrycnt,
				Nday:     req.Nday,
				Sdate:    req.Sdate,
				Stime:    "",
				Edate:    req.Edate,
				Etime:    "",
				Cts_Date: ctsDate,
				Cts_Time: ctsTime,
				Comp_yn:  req.Comp_yn,
			},
		},
		header: restHeaderSpec{
			trCont:    trCont,
			trContKey: trContKey,
		},
		resultHeader: header,
		resultBody:   &resp,
	}); err != nil {
		return nil, nil, err
	}
	return &resp, header, nil
}

// KRX, NXT 거래소 통합 분봉 조회.
// 응답은 edate 기준으로 내림차순 정렬되어 오며, 연속조회는 edate 이전 데이터로 조회됨.
//
// 각 페이지당 응답 예시:
//
//	edate: 당일, sdate: " ", qrycnt: 500
//	[
//	  ...
//	  {"time": "152900"},
//	  {"time": "153000"}
//	]
//
// 구조가 됨.
func (svc *REST) Chart_통합_주식차트_N분(ctx context.Context, req ChartMinuteRequest) (requester.Header, *T8452Response, error) {
	var merged *T8452Response
	nextTrCont := "N"
	nextTrContKey := ""
	nextCtsDate := " "
	nextCtsTime := " "

	var lastHeader requester.Header
	if req.PageTimeout <= 0 {
		return nil, nil, InVaildPageTimeout
	}
	for {
		pageCtx, cancel := context.WithTimeout(ctx, req.PageTimeout)
		var resp T8452Response
		header := make(requester.Header)
		if err := svc.sendREST(pageCtx, restRequest{
			method: requester.PostMethod,
			path:   PathStockChart,
			trID:   TrIDChart_Integration_Stock_Minute,
			bodyReq: t8452Request{
				In: t8452InBlock{
					Shcode:    req.Shcode,
					Ncnt:      req.Minute,
					Qrycnt:    req.Qrycnt,
					Nday:      req.Nday,
					Sdate:     req.Sdate,
					Stime:     "",
					Edate:     req.Edate,
					Etime:     "",
					Cts_Date:  nextCtsDate,
					Cts_Time:  nextCtsTime,
					Comp_yn:   req.Comp_yn,
					Exchgubun: req.Exchgubun,
				},
			},
			header: restHeaderSpec{
				trCont:    nextTrCont,
				trContKey: nextTrContKey,
			},
			resultHeader: header,
			resultBody:   &resp,
		}); err != nil {
			cancel()
			return nil, nil, err
		}
		cancel()
		lastHeader = header

		if merged == nil {
			merged = &resp

		} else {
			merged.T8452OutBlock = resp.T8452OutBlock
			merged.T8452OutBlock1 = append(resp.T8452OutBlock1, merged.T8452OutBlock1...)
		}

		nextTrCont = header.Get("tr_cont")

		// 종료 조건: 연속조회 비활성 OR 헤더가 "Y" 아님 OR 연속키 소진
		if !req.Cont || nextTrCont != "Y" {
			return lastHeader, merged, nil
		}
		nextTrContKey = header.Get("tr_cont_key")
		nextCtsDate = resp.T8452OutBlock.Cts_Date
		// api 문서상으로는 응갑 Cts_Time이용하라 되어 있는데
		// Cts_Time을 요청 cts_time으로 넘기면 해당 time 이전부터 조회됨.
		// 그래서 연속조회 "Y" 일경우에 첫배열 요소 time을 다음 페이지 cts_time으로 넘겨서 조회함.
		// 개떡같은 문서;;
		nextCtsTime = resp.T8452OutBlock1[0].Time
	}
}

// KRX, NXT 거래소 통합 틱(N틱) 차트 조회.
// 응답은 edate 기준으로 내림차순 정렬되어 오며, 연속조회는 edate 이전 데이터로 조회됨.
//
// req.Minute 필드는 t8453 의 ncnt(틱 단위)로 사용됨.
func (svc *REST) Chart_통합_주식차트_N틱(ctx context.Context, req ChartTickRequest) (requester.Header, *T8453Response, error) {
	var merged *T8453Response
	nextTrCont := "N"
	nextTrContKey := ""
	nextCtsDate := " "
	nextCtsTime := " "

	var lastHeader requester.Header
	if req.PageTimeout <= 0 {
		return nil, nil, InVaildPageTimeout
	}
	for {
		pageCtx, cancel := context.WithTimeout(ctx, req.PageTimeout)
		var resp T8453Response
		header := make(requester.Header)
		if err := svc.sendREST(pageCtx, restRequest{
			method: requester.PostMethod,
			path:   PathStockChart,
			trID:   TrIDChart_Integration_Stock_Tick,
			bodyReq: t8453Request{
				In: t8453InBlock{
					Shcode:    req.Shcode,
					Ncnt:      req.Ncnt,
					Qrycnt:    req.Qrycnt,
					Nday:      req.Nday,
					Sdate:     req.Sdate,
					Stime:     "",
					Edate:     req.Edate,
					Etime:     "",
					Cts_Date:  nextCtsDate,
					Cts_Time:  nextCtsTime,
					Comp_yn:   req.Comp_yn,
					Exchgubun: req.Exchgubun,
				},
			},
			header: restHeaderSpec{
				trCont:    nextTrCont,
				trContKey: nextTrContKey,
			},
			resultHeader: header,
			resultBody:   &resp,
		}); err != nil {
			cancel()
			return nil, nil, err
		}
		cancel()
		lastHeader = header

		if merged == nil {
			merged = &resp

		} else {
			merged.T8453OutBlock = resp.T8453OutBlock
			merged.T8453OutBlock1 = append(resp.T8453OutBlock1, merged.T8453OutBlock1...)
		}

		nextTrCont = header.Get("tr_cont")

		// 종료 조건: 연속조회 비활성 OR 헤더가 "Y" 아님 OR 연속키 소진
		if !req.Cont || nextTrCont != "Y" {
			return lastHeader, merged, nil
		}
		nextTrContKey = header.Get("tr_cont_key")
		nextCtsDate = resp.T8453OutBlock.Cts_Date
		// api 문서상으로는 응갑 Cts_Time이용하라 되어 있는데
		// Cts_Time을 요청 cts_time으로 넘기면 해당 time 이전부터 조회됨.
		// 그래서 연속조회 "Y" 일경우에 첫배열 요소 time을 다음 페이지 cts_time으로 넘겨서 조회함.
		// 개떡같은 문서;;
		nextCtsTime = resp.T8453OutBlock1[0].Time
	}
}
