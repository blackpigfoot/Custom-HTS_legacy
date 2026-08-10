package component

import (
	"context"
	"fmt"
	"strings"
	"time"

	lsapi "Custom-HTS/internal/adapter/broker/ls/api"
)

// RunLocalTest assembles the client once and dispatches to a small smoke-test flow.
func RunLocalTest() error {
	// cfg collects the common smoke-test inputs from .env and quick inline overrides.
	cfg, err := loadLocalTestConfig()
	if err != nil {
		return err
	}

	// ctx bounds the full smoke-test run so hanging network calls fail quickly.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// client is the shared LS API client used by the selected smoke-test flow.
	client, err := lsapi.New(lsapi.Config{
		AppKey:    cfg.AppKey,
		AppSecret: cfg.AppSecret,
		AccountNo: cfg.AccountNo,
	})
	if err != nil {
		return err
	}
	var modeerr error
	now := time.Now() // mark the client creation time, which may include token acquisition.1
	switch strings.ToLower(cfg.Mode) {
	case ChartMinute:
		modeerr = runChartMinuteTest(ctx, client)
	case Multi:
		modeerr = run_Stock_Muilti_Price(ctx, client)
	case StockPriceByMinute:
		modeerr = runStock_Price_By_Minute(ctx, client)
	case Chart_Integration_Stock_Minute:
		modeerr = runChart_Integration_Stock_Minute(ctx, client)
	case Chart_Integration_Stock_Tick:
		modeerr = runChart_Integration_Stock_Tick(ctx, client)
	default:
		modeerr = fmt.Errorf("unknown LS test mode: %s", cfg.Mode)
	}
	elapsed := time.Since(now)
	if modeerr != nil {
		return fmt.Errorf("%s test failed after %s: %w", cfg.Mode, elapsed, modeerr)
	}
	fmt.Printf("%s success after %s", cfg.Mode, elapsed)
	return modeerr
}
