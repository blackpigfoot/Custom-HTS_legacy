package api

import (
	"context"
	"strings"

	"Custom-HTS/internal/core/pkg/requester"
)

// Stock_당일분별주가조회 returns the t1302 minute-by-minute price response.
//
// LS t1302는 분별 주가를 연속조회로 제공한다. cont=true이면 응답이 더 있을 때까지
// 자동으로 페이지를 이어 받는다 (헤더의 tr_cont == "Y" 동안 반복).
//
// 인자:
//
//	code: 종목코드 (예: "005930")
//	cont: 연속조회 자동 수행 여부. false면 1페이지만 반환.
//
// 반환:
//
//	첫째: 마지막 응답의 헤더 (디버깅·tr_cont 상태 확인용)
//	둘째: 누적된 응답 (OutBlock은 마지막 페이지 기준, OutBlock1은 전체 append)
func (svc *REST) Stock_당일분별주가조회(ctx context.Context, code string, cont bool) (map[string][]string, *t1302Response, error) {
	// merged는 첫 응답을 기준으로 두고 이후 페이지의 리스트만 append.
	var merged *t1302Response

	// nextTrCont/nextTrContKey는 직전 응답의 헤더에서 다음 요청에 전달할 연속조회 값.
	nextTrCont := "N"
	nextTrContKey := ""
	// nextCtsTime은 직전 응답의 body에서 추출한 다음 페이지의 시작 시각.
	// t1302의 continuation key 필드명이 다르면 (예: cts_time, cts_date 등) 여기 교체.
	nextCtsTime := ""

	// lastHeader는 가장 최근 호출의 응답 헤더를 보관 (반환용).
	var lastHeader requester.Header
	for {
		// resp는 한 페이지의 디코드된 응답.
		var resp t1302Response
		header := make(requester.Header)
		if err := svc.sendREST(ctx, restRequest{
			method: requester.PostMethod,
			path:   PathStockMarket,
			trID:   TrIDStockPriceByMinute,
			bodyReq: t1302Request{
				In: t1302InBlock{
					Shcode: code,
					Gubun:  "1", // 1: 분별 주가

					Time:      nextCtsTime, // 첫 호출은 ""
					Cnt:       900,
					Exchgubun: "U",
				},
			},
			header: restHeaderSpec{
				trCont:    nextTrCont,
				trContKey: nextTrContKey,
			},
			resultHeader: header,
			resultBody:   &resp,
		}); err != nil {
			return nil, nil, &OperationError{
				Op:  "ls get stock price by minute",
				Err: err,
			}
		}
		lastHeader = header

		if merged == nil {
			merged = &resp
		} else {
			// OutBlock은 마지막 페이지 기준으로 갱신 (메타 정보가 변경될 수 있음).
			merged.T1302OutBlock = resp.T1302OutBlock
			// OutBlock1 (분봉 리스트)은 누적.
			merged.T1302OutBlock1 = append(merged.T1302OutBlock1, resp.T1302OutBlock1...)
		}

		nextTrCont = header.Get("tr_cont")
		nextTrContKey = header.Get("tr_cont_key")
		nextCtsTime = resp.T1302OutBlock.Cts_Time // 필드명 확인 필요

		// 종료 조건: cont 비활성 OR 헤더가 "Y"가 아님 OR continuation key 없음
		if !cont || !strings.EqualFold(nextTrCont, "Y") || nextCtsTime == "" {
			return lastHeader, merged, nil
		}
	}
}
