package domain

import "time"

// TickMsg is the domain-level trade tick emitted by broker wrappers.
type TickMsg struct {
	// BrokerID identifies the managed broker instance that emitted the tick.
	BrokerID string
	// AccountID identifies the account when the tick is account-scoped.
	AccountID string
	// Code is the issue code.
	Code string
	// Price is the current price in KRW scale=1.
	Price int64
	// Volume is the trade volume for this tick.
	Volume int64
	// AccVolume is the cumulative session volume.
	AccVolume int64
	// Change is the price change from the previous close.
	Change int64
	// ChangeRate is the fixed-point percent change.
	ChangeRate int64
	// TradeTime is the vendor-native trade time when available.
	TradeTime string
	// ReceivedAt is the local time when the wrapper accepted the message.
	ReceivedAt time.Time
	// Raw keeps the vendor-native source DTO.
	Raw any
}

// ExecMsg is the domain-level order and execution notification.
type ExecMsg struct {
	// BrokerID identifies the managed broker instance that emitted the message.
	BrokerID string
	// AccountID identifies the logical account.
	AccountID string
	// OrderID is the broker-issued order identifier.
	OrderID string
	// Code is the issue code.
	Code string
	// Side is the execution side.
	Side Side
	// Price is the execution price in KRW scale=1.
	Price int64
	// Quantity is the execution quantity.
	Quantity int64
	// RemainQty is the unfilled quantity.
	RemainQty int64
	// TotalQty is the original order quantity.
	TotalQty int64
	// Status is the vendor or normalized execution status.
	Status string
	// ExecTime is the execution timestamp when available.
	ExecTime time.Time
	// ReceivedAt is the local time when the wrapper accepted the message.
	ReceivedAt time.Time
	// Raw keeps the vendor-native source DTO.
	Raw any
}

// QuoteMsg is the domain-level orderbook update.
type QuoteMsg struct {
	// BrokerID identifies the managed broker instance that emitted the quote.
	BrokerID string
	// AccountID identifies the account when the quote is account-scoped.
	AccountID string
	// Code is the issue code.
	Code string
	// Asks stores ask levels from best to deeper levels.
	Asks [10]PriceLevel
	// Bids stores bid levels from best to deeper levels.
	Bids [10]PriceLevel
	// TotalAsk is the total ask quantity.
	TotalAsk int64
	// TotalBid is the total bid quantity.
	TotalBid int64
	// ReceivedAt is the local time when the wrapper accepted the message.
	ReceivedAt time.Time
	// Raw keeps the vendor-native source DTO.
	Raw any
}

// LifecycleMsg reports the wrapper-level lifecycle of a managed broker.
type LifecycleMsg struct {
	// BrokerID identifies the managed broker instance.
	BrokerID string
	// AccountID identifies the logical account.
	AccountID string
	// Status is the broker lifecycle state.
	Status BrokerStatus
	// Message describes the lifecycle transition.
	Message string
	// Err is the underlying error when the lifecycle transition failed.
	Err error
	// ReceivedAt is the local time when the lifecycle event was recorded.
	ReceivedAt time.Time
}
