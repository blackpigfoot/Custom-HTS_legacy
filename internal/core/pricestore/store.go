// Package pricestore — 종목별 최신가 메모리 캐시.
//
// Engine이 tick 수신 시 Update 호출, Strategy가 Get으로 조회.
// Engine 단일 고루틴에서만 접근하므로 락 불필요.
// 자체 Run 루프 없음.
package pricestore

import (
	"time"

	"Custom-HTS/internal/core/domain"
)

// Store — 종목별 최신가 캐시.
type Store struct {
	prices map[string]*Snapshot
}

// Snapshot — 종목 하나의 최신 시세.
type Snapshot struct {
	Code       string
	Price      int64
	Volume     int64  // 직전 체결수량
	AccVolume  int64  // 누적거래량
	Change     int64  // 전일대비 (KRW)
	ChangeRate int64  // 등락률 (×RateScale)
	TradeTime  string // 거래소 원본 시간 문자열
	UpdatedAt  time.Time
}

func New() *Store {
	return &Store{
		prices: make(map[string]*Snapshot),
	}
}

// Update — tick 수신 시 호출. 해당 종목의 스냅샷을 덮어쓴다.
// 이전 값을 유지할 필요 없음 — 최신가만 의미 있음.
func (s *Store) Update(tick domain.TickEvent) {
	s.prices[tick.Code] = &Snapshot{
		Code:       tick.Code,
		Price:      tick.Price,
		Volume:     tick.Volume,
		AccVolume:  tick.AccVolume,
		Change:     tick.Change,
		ChangeRate: tick.ChangeRate,
		TradeTime:  tick.TradeTime,
		UpdatedAt:  tick.ReceivedAt,
	}
}

// Get — 종목 최신가 조회. 미수신 종목은 nil 반환.
func (s *Store) Get(code string) *Snapshot {
	return s.prices[code]
}

// GetAll — 전체 종목 스냅샷 복사본. UI push 등에 사용.
func (s *Store) GetAll() map[string]*Snapshot {
	cp := make(map[string]*Snapshot, len(s.prices))
	for k, v := range s.prices {
		snap := *v
		cp[k] = &snap
	}
	return cp
}

// Has — 해당 종목의 시세를 한 번이라도 수신했는지 확인.
func (s *Store) Has(code string) bool {
	_, ok := s.prices[code]
	return ok
}
