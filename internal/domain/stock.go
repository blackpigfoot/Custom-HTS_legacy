// internal/domain/stock.go (의사코드)
package domain

type Stock struct {
    Code string `json:"code"`
    Name string `json:"name"`
    // 시세는 universe에서 추가 — 시세 부착 시점에 별도 타입 또는 필드 확장
}