package component

import (
	"fmt"
	"strings"
	"time"

	"Custom-HTS/internal/core/pkg/env"
)

const (
	Token                          = "token"
	Price                          = "price"
	Orderbook                      = "orderbook"
	Multi                          = "multi"
	Balance                        = "balance"
	ChartMinute                    = "chart_minute"
	InvestWarning                  = "invest_warning"
	StockPriceByMinute             = "stock_price_by_minute"
	Chart_Integration_Stock_Tick   = "stock_price_by_tick"
	Chart_Integration_Stock_Minute = "chart_integration_stock_minute"
	// defaultTestCode is the fallback issue code used by quote-oriented smoke tests.
	defaultTestCode = "005930"
	// defaultTestTimeout is the fallback timeout used for one local smoke-test run.
	defaultTestTimeout = 5 * time.Second
)

var (
	// multiTestCodes is the default list of issue codes used by the multi-price smoke test.
	multiTestCodes = []string{"005930", "060370", "000500", "475430", "103590"}
)

var (
	// testMode selects which local smoke test runs.
	testMode = Chart_Integration_Stock_Minute
	// testCode selects the issue code used by single-code quote-oriented smoke tests.
	testCode = defaultTestCode
	// testMultiCodes selects the issue-code list used by the multi-price smoke test.
	testMultiCodes = multiTestCodes
)

// localTestConfig stores only the inputs shared by every local smoke test.
type localTestConfig struct {
	// AppKey is the LS OpenAPI application key used by the smoke test.
	AppKey string
	// AppSecret is the LS OpenAPI application secret used by the smoke test.
	AppSecret string
	// AccountNo is the optional LS account number used by account-scoped smoke tests.
	AccountNo string
	// Mode selects the smoke-test flow such as token, price, orderbook, multi, or balance.
	Mode string
	// Timeout bounds one smoke-test run.
	Timeout time.Duration
}

// loadLocalTestConfig loads the common smoke-test configuration from the LS
// account 1 credentials in .env and the inline test defaults.
func loadLocalTestConfig() (localTestConfig, error) {
	if err := env.Load(); err != nil {
		return localTestConfig{}, fmt.Errorf("load .env: %w", err)
	}
	if missing := env.Missing("LS_APP_KEY_1", "LS_APP_SECRET_1"); len(missing) > 0 {
		return localTestConfig{}, fmt.Errorf("missing environment variables: %s (copy .env.example to .env and fill them in)", strings.Join(missing, ", "))
	}

	return localTestConfig{
		AppKey:    env.Get("LS_APP_KEY_1"),
		AppSecret: env.Get("LS_APP_SECRET_1"),
		AccountNo: env.Get("LS_ACCOUNT_NO_1"),
		Mode:      testMode,
		Timeout:   defaultTestTimeout,
	}, nil
}
