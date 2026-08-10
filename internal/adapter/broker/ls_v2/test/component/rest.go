package component

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	lsapi "Custom-HTS/internal/adapter/broker/ls/api"
)

func fmtPrint(v any) {
	switch v := v.(type) {
	case string:
		fmt.Println(v)
	case []byte:
		fmt.Println(string(v))
	case map[string]string:
		for k, v := range v {
			fmt.Printf("%s: %s\n", k, v)
		}
	default:
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			fmt.Printf("json marshal failed: %v\n", err)
			return
		}
		fmt.Println(string(b))
	}
}

func run_Stock_Muilti_Price(ctx context.Context, client *lsapi.API) error {
	_, body, err := client.GetMultiPrices(ctx, []string{"005930"})
	if err != nil {
		return err
	}
	fmtPrint(body.OutBlock1)
	return nil
}

func runChartMinuteTest(_ context.Context, client *lsapi.API) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// chartResp is the vendor-native t8412 response payload.
	_, chartResp, err := client.ChartMinute(ctx, lsapi.ChartMinuteRequest{
		Shcode:      "037270",
		Minute:      1,
		Qrycnt:      500,
		Nday:        "0",
		Sdate:       "20260522",
		Edate:       "999999",
		Comp_yn:     "N",
		Cont:        true,
		PageTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	fmtPrint(chartResp.T8412OutBlock1[0:3])
	return nil
}

func runStock_Price_By_Minute(ctx context.Context, client *lsapi.API) error {
	// startedAt marks the full REST call start time, including network and decode work.
	startedAt := time.Now()
	// priceByMinuteResp is the vendor-native t1302 response payload.
	_, body, err := client.Stock_당일분별주가조회(ctx, "037270", false)
	// elapsed records the total client-call cost including network and parsing time.
	elapsed := time.Since(startedAt)
	if err != nil {
		return fmt.Errorf("stock-price-by-minute request failed after %s: %w", elapsed, err)
	}
	fmtPrint(len(body.T1302OutBlock1))
	fmtPrint(body.T1302OutBlock1[0:30])
	return nil
}

func runChart_Integration_Stock_Minute(ctx context.Context, client *lsapi.API) error {
	// chartResp is the vendor-native t8452 response payload.
	_, resp, err := client.Chart_통합_주식차트_N분(ctx, lsapi.ChartMinuteRequest{
		Shcode:      "402030",
		Minute:      1,
		Qrycnt:      500,
		Nday:        "0",
		Sdate:       "20260529",
		Edate:       "99999999",
		Comp_yn:     "N",
		Cont:        true,
		PageTimeout: 5 * time.Second,
		Exchgubun:   "K",
	})
	if err != nil {
		return err
	}
	fmtPrint(resp.T8452OutBlock1[0])

	fmtPrint(len(resp.T8452OutBlock1))

	return nil
}

func runChart_Integration_Stock_Tick(ctx context.Context, client *lsapi.API) error {
	// chartResp is the vendor-native t8452 response payload.
	_, resp, err := client.Chart_통합_주식차트_N틱(ctx, lsapi.ChartTickRequest{
		Shcode:      "005930",
		Ncnt:        100,
		Qrycnt:      500,
		Nday:        "0",
		Sdate:       "20260522",
		Edate:       "99999999",
		Comp_yn:     "N",
		Cont:        true,
		PageTimeout: 5 * time.Second,
		Exchgubun:   "K",
	})
	if err != nil {
		return err
	}
	fmtPrint(resp.T8453OutBlock)
	fmtPrint(len(resp.T8453OutBlock1))
	fmtPrint(resp.T8453OutBlock1[len(resp.T8453OutBlock1)-1])
	return nil
}
