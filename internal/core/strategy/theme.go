// Package strategy — 매매 전략 구현.
//
// engine.Strategy 인터페이스를 구현. engine을 import하지 않음(순환 방지).
// pricestore, ordertracker, balancemgr를 읽기 참조하여 판단.
package strategy

import (
	"encoding/json"
	"fmt"
	"os"

	"Custom-HTS/internal/core/balancemgr"
	"Custom-HTS/internal/core/domain"
	"Custom-HTS/internal/core/ordertracker"
	"Custom-HTS/internal/core/pkg/logger"
	"Custom-HTS/internal/core/pricestore"
)

// RateScale — domain.TickEvent.ChangeRate와 동일한 스케일.
// 1.25% = 12500.
const RateScale int64 = 10000

// ThemeStrategy — 테마주 매매 전략.
//
// 동작 원리:
//  1. 테마 내 여러 종목의 등락률이 각각의 임계값을 모두 충족(AND) → 매수 신호
//  2. 보유 종목의 수익률이 profitPct 이상 → 익절 신호
//  3. 보유 종목의 손실률이 lossPct 이상 → 손절 신호
//
// 예시 규칙:
//
//	조건: 1등주 +8% AND 2등주 +5% AND 3등주 +4%
//	행동: 1등주 매수
//
// 제약:
//   - 동일 종목 미체결 주문이 있으면 중복 매수 안 함
//   - 이미 보유 중이면 추가 매수 안 함
type ThemeStrategy struct {
	themes []*ThemeGroup
	rules  []*ThemeRule

	prices   *pricestore.Store
	orders   *ordertracker.Tracker
	balances *balancemgr.Manager

	log *logger.Logger
}

// ThemeGroup — 테마 하나. 파일에서 로드.
type ThemeGroup struct {
	Name   string       `json:"name"`
	Stocks []ThemeStock `json:"stocks"`
}

// ThemeStock — 테마 내 종목. rank가 낮을수록 대장주.
type ThemeStock struct {
	Rank int    `json:"rank"`
	Code string `json:"code"`
	Name string `json:"name"`
}

// ThemeRule — 테마별 매매 규칙.
//
// Conditions: 모든 조건이 AND로 충족되어야 매수 발동.
// BuyTargets: 조건 충족 시 매수할 종목들.
type ThemeRule struct {
	ThemeName  string
	Conditions []RankCondition // 전부 충족해야 함 (AND)
	BuyTargets []BuyTarget     // 매수 대상
	ProfitPct  float64         // 익절 기준 (2.0 = 2%)
	LossPct    float64         // 손절 기준 (1.5 = 1.5%, 양수 입력)
}

// RankCondition — 특정 순위 종목의 등락률 조건.
//
// 예: {Rank: 1, MinPct: 8.0} → 1등주가 +8% 이상이어야 함
type RankCondition struct {
	Rank   int
	MinPct float64 // 최소 등락률 (%)
}

// BuyTarget — 조건 충족 시 매수할 종목.
type BuyTarget struct {
	Rank      int   // 테마 내 순위 (매수 대상)
	BuyAmount int64 // 매수 금액 (원). 수량은 현재가로 계산.
}

// ThemeStrategyDeps — 생성 시 주입할 읽기 참조.
type ThemeStrategyDeps struct {
	Prices   *pricestore.Store
	Orders   *ordertracker.Tracker
	Balances *balancemgr.Manager
	Log      *logger.Logger
}

// NewThemeStrategy — 테마 파일 로드 + 하드코딩 규칙으로 전략 생성.
func NewThemeStrategy(themePath string, deps ThemeStrategyDeps) (*ThemeStrategy, error) {
	themes, err := loadThemeFile(themePath)
	if err != nil {
		return nil, fmt.Errorf("loading theme file: %w", err)
	}

	rules := buildDefaultRules(themes)

	s := &ThemeStrategy{
		themes:   themes,
		rules:    rules,
		prices:   deps.Prices,
		orders:   deps.Orders,
		balances: deps.Balances,
		log:      deps.Log,
	}

	deps.Log.Info("theme strategy initialized",
		"themes", len(themes),
		"rules", len(rules),
	)

	return s, nil
}

// OnTick — 틱 수신 시 매매 판단.
//
// 매 틱마다:
//  1. 이 틱의 종목이 속한 테마의 전체 조건 확인 → 모두 충족 시 매수 신호
//  2. 보유 종목이면 익절/손절 확인 → 매도 신호
func (s *ThemeStrategy) OnTick(tick domain.TickEvent) []*domain.TradeSignal {
	var signals []*domain.TradeSignal

	for _, rule := range s.rules {
		if !s.isStockInTheme(rule.ThemeName, tick.Code) {
			continue
		}
		if !s.checkAllConditions(rule) {
			continue
		}
		buys := s.generateBuySignals(rule)
		signals = append(signals, buys...)
	}

	exits := s.checkExitConditions(tick)
	signals = append(signals, exits...)

	return signals
}

// OnExec — 체결통보 수신 시. 현재 추가 판단 없음.
func (s *ThemeStrategy) OnExec(_ domain.ExecEvent) []*domain.TradeSignal {
	return nil
}

// checkAllConditions — 규칙의 모든 조건이 충족되는지 확인 (AND).
//
// 각 조건의 rank에 해당하는 종목의 현재 등락률을 pricestore에서 조회.
// 하나라도 미충족이면 false.
func (s *ThemeStrategy) checkAllConditions(rule *ThemeRule) bool {
	for _, cond := range rule.Conditions {
		stock := s.findStock(rule.ThemeName, cond.Rank)
		if stock == nil {
			return false
		}

		snap := s.prices.Get(stock.Code)
		if snap == nil {
			return false
		}

		threshold := pctToRate(cond.MinPct)
		if snap.ChangeRate < threshold {
			return false
		}
	}
	return true
}

// generateBuySignals — 조건 충족 후 매수 대상 종목별 신호 생성.
func (s *ThemeStrategy) generateBuySignals(rule *ThemeRule) []*domain.TradeSignal {
	var signals []*domain.TradeSignal

	s.logConditionsMet(rule)

	for _, target := range rule.BuyTargets {
		stock := s.findStock(rule.ThemeName, target.Rank)
		if stock == nil {
			continue
		}

		if h := s.balances.GetHolding("", stock.Code); h != nil && h.Quantity > 0 {
			s.log.Debug("skip buy: already holding",
				"code", stock.Code,
				"qty", h.Quantity,
			)
			continue
		}

		if s.orders.HasOpenOrder("", stock.Code) {
			s.log.Debug("skip buy: open order exists", "code", stock.Code)
			continue
		}

		snap := s.prices.Get(stock.Code)
		if snap == nil || snap.Price <= 0 {
			s.log.Debug("skip buy: no price data", "code", stock.Code)
			continue
		}

		qty := target.BuyAmount / snap.Price
		if qty <= 0 {
			s.log.Debug("skip buy: quantity too small",
				"code", stock.Code,
				"price", snap.Price,
				"buyAmount", target.BuyAmount,
			)
			continue
		}

		reason := s.buildBuyReason(rule, stock, target.Rank)

		signals = append(signals, &domain.TradeSignal{
			Code:     stock.Code,
			Side:     domain.Buy,
			Type:     domain.Limit,
			Quantity: qty,
			Price:    snap.Price,
			Reason:   reason,
		})
	}

	return signals
}

// checkExitConditions — 보유 종목의 익절/손절 확인.
func (s *ThemeStrategy) checkExitConditions(tick domain.TickEvent) []*domain.TradeSignal {
	var signals []*domain.TradeSignal

	rule := s.findRuleForTarget(tick.Code)
	if rule == nil {
		return nil
	}

	accountID, h := s.balances.FindHoldingWithAccount(tick.Code)
	if h == nil || h.Quantity <= 0 {
		return nil
	}

	avgCost := h.AvgCost
	if avgCost <= 0 {
		return nil
	}

	pnlRate := (tick.Price - avgCost) * RateScale / avgCost

	profitRate := pctToRate(rule.ProfitPct)
	lossRate := pctToRate(rule.LossPct)

	if pnlRate >= profitRate {
		signals = append(signals, &domain.TradeSignal{
			AccountID: accountID,
			Code:      tick.Code,
			Side:      domain.Sell,
			Type:      domain.Market,
			Quantity:  h.Quantity,
			Reason: fmt.Sprintf("익절: %s 수익률 %s%% (목표 %.1f%%)",
				tick.Code, fmtRate(pnlRate), rule.ProfitPct),
		})
	} else if pnlRate <= -lossRate {
		signals = append(signals, &domain.TradeSignal{
			AccountID: accountID,
			Code:      tick.Code,
			Side:      domain.Sell,
			Type:      domain.Market,
			Quantity:  h.Quantity,
			Reason: fmt.Sprintf("손절: %s 손실률 %s%% (기준 -%.1f%%)",
				tick.Code, fmtRate(pnlRate), rule.LossPct),
		})
	}

	return signals
}

// logConditionsMet — 조건 충족 시 각 종목 등락률 로깅.
func (s *ThemeStrategy) logConditionsMet(rule *ThemeRule) {
	for _, cond := range rule.Conditions {
		stock := s.findStock(rule.ThemeName, cond.Rank)
		if stock == nil {
			continue
		}
		snap := s.prices.Get(stock.Code)
		if snap == nil {
			continue
		}
		s.log.Info("condition met",
			"theme", rule.ThemeName,
			"rank", cond.Rank,
			"code", stock.Code,
			"name", stock.Name,
			"changeRate", fmtRate(snap.ChangeRate)+"%",
			"threshold", fmt.Sprintf("%.1f%%", cond.MinPct),
		)
	}
}

// buildBuyReason — 매수 사유 문자열 생성.
func (s *ThemeStrategy) buildBuyReason(rule *ThemeRule, target *ThemeStock, targetRank int) string {
	condStr := ""
	for i, cond := range rule.Conditions {
		stock := s.findStock(rule.ThemeName, cond.Rank)
		snap := s.prices.Get(stock.Code)
		if i > 0 {
			condStr += " & "
		}
		condStr += fmt.Sprintf("%d등주 %s%%", cond.Rank, fmtRate(snap.ChangeRate))
	}
	return fmt.Sprintf("테마[%s] %s → %s(rank%d) 매수",
		rule.ThemeName, condStr, target.Name, targetRank)
}

// isStockInTheme — 종목코드가 해당 테마에 속하는지 확인.
func (s *ThemeStrategy) isStockInTheme(themeName, code string) bool {
	for _, theme := range s.themes {
		if theme.Name != themeName {
			continue
		}
		for _, stock := range theme.Stocks {
			if stock.Code == code {
				return true
			}
		}
	}
	return false
}

// findStock — 테마명 + 순위로 종목 조회.
func (s *ThemeStrategy) findStock(themeName string, rank int) *ThemeStock {
	for _, theme := range s.themes {
		if theme.Name != themeName {
			continue
		}
		for i := range theme.Stocks {
			if theme.Stocks[i].Rank == rank {
				return &theme.Stocks[i]
			}
		}
	}
	return nil
}

// findRuleForTarget — 종목코드가 BuyTarget에 포함된 rule 조회 (익절/손절 기준 참조용).
func (s *ThemeStrategy) findRuleForTarget(code string) *ThemeRule {
	for _, rule := range s.rules {
		for _, target := range rule.BuyTargets {
			stock := s.findStock(rule.ThemeName, target.Rank)
			if stock != nil && stock.Code == code {
				return rule
			}
		}
	}
	return nil
}

// buildDefaultRules — 하드코딩 기본 규칙.
// 나중에 JSON/YAML 설정 파일로 교체. 교체 지점: 이 함수 한 곳.
//
// 예시 규칙:
//
//	조건: 1등주 +8% AND 2등주 +5% AND 3등주 +4%
//	행동: 1등주를 100만원어치 매수
//	익절: +2%, 손절: -1.5%
func buildDefaultRules(themes []*ThemeGroup) []*ThemeRule {
	var rules []*ThemeRule
	for _, theme := range themes {
		if len(theme.Stocks) < 2 {
			continue
		}

		rules = append(rules, &ThemeRule{
			ThemeName: theme.Name,
			Conditions: []RankCondition{
				{Rank: 1, MinPct: 8.0},
				{Rank: 2, MinPct: 5.0},
				{Rank: 3, MinPct: 4.0},
			},
			BuyTargets: []BuyTarget{
				{Rank: 1, BuyAmount: 1_000_000},
			},
			ProfitPct: 2.0,
			LossPct:   1.5,
		})
	}
	return rules
}

// loadThemeFile — 테마 JSON 파일 로드.
func loadThemeFile(path string) ([]*ThemeGroup, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	var file struct {
		Themes []*ThemeGroup `json:"themes"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	if len(file.Themes) == 0 {
		return nil, fmt.Errorf("no themes in %s", path)
	}

	return file.Themes, nil
}

func pctToRate(pct float64) int64 {
	return int64(pct * float64(RateScale))
}

func fmtRate(rate int64) string {
	return fmt.Sprintf("%.2f", float64(rate)/float64(RateScale))
}
