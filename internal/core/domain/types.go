// Package domain contains application-level shared trading types.
package domain

import "time"

// Side identifies the direction of an order or execution.
type Side string

const (
	// Buy represents a buy-side action.
	Buy Side = "buy"
	// Sell represents a sell-side action.
	Sell Side = "sell"
)

// OrderType identifies how an order should be executed.
type OrderType string

const (
	// Limit represents a limit order with an explicit price.
	Limit OrderType = "limit"
	// Market represents a market order.
	Market OrderType = "market"
)

// OrderStatus identifies the application-level order lifecycle state.
type OrderStatus string

const (
	// StatusPending means the order is waiting for broker submission or acknowledgement.
	StatusPending OrderStatus = "pending"
	// StatusOpen means the order is accepted and still open.
	StatusOpen OrderStatus = "open"
	// StatusFilled means the order is fully filled.
	StatusFilled OrderStatus = "filled"
	// StatusPartial means the order is partially filled.
	StatusPartial OrderStatus = "partial"
	// StatusCancelled means the order was cancelled.
	StatusCancelled OrderStatus = "cancelled"
	// StatusRejected means the broker rejected the order.
	StatusRejected OrderStatus = "rejected"
)

// BrokerStatus identifies the lifecycle state of a managed broker instance.
type BrokerStatus string

const (
	// BrokerStatusCreated means the broker object exists but has not started.
	BrokerStatusCreated BrokerStatus = "created"
	// BrokerStatusStarting means Start was requested and startup is in progress.
	BrokerStatusStarting BrokerStatus = "starting"
	// BrokerStatusRunning means the broker is active.
	BrokerStatusRunning BrokerStatus = "running"
	// BrokerStatusStopping means Stop or Remove was requested.
	BrokerStatusStopping BrokerStatus = "stopping"
	// BrokerStatusStopped means the broker is no longer active.
	BrokerStatusStopped BrokerStatus = "stopped"
	// BrokerStatusFailed means the broker entered an unrecoverable local state.
	BrokerStatusFailed BrokerStatus = "failed"
)

// PriceLevel stores one quote level.
type PriceLevel struct {
	// Price is the level price in KRW scale=1.
	Price int64
	// Size is the level quantity.
	Size int64
}

// Price stores a common current-price snapshot.
type Price struct {
	// Code is the issue code.
	Code string
	// Current is the current price in KRW scale=1.
	Current int64
	// Open is the session open price.
	Open int64
	// High is the session high price.
	High int64
	// Low is the session low price.
	Low int64
	// PrevClose is the previous close price.
	PrevClose int64
	// Volume is the cumulative volume.
	Volume int64
	// Change is the price change from the previous close.
	Change int64
	// ChangeRate is the fixed-point percent change.
	ChangeRate int64
	// Timestamp is the local snapshot time.
	Timestamp time.Time
	// Raw keeps the vendor-native source DTO.
	Raw any
}

// OrderRequest stores a common order intent.
type OrderRequest struct {
	// Code is the issue code.
	Code string
	// Side is the order direction.
	Side Side
	// Type is the order execution type.
	Type OrderType
	// Quantity is the requested share quantity.
	Quantity int64
	// Price is the limit price, or zero for market orders.
	Price int64
}

// OrderResponse stores a common order submission response.
type OrderResponse struct {
	// OrderID is the broker-issued order identifier.
	OrderID string
	// Code is the issue code.
	Code string
	// Side is the order direction.
	Side Side
	// Type is the order execution type.
	Type OrderType
	// Quantity is the submitted share quantity.
	Quantity int64
	// Price is the submitted limit price.
	Price int64
	// Status is the initial application-level order state.
	Status OrderStatus
	// CreatedAt is the local submission timestamp.
	CreatedAt time.Time
	// Raw keeps the vendor-native source DTO.
	Raw any
}

// Order stores a common open-order or order-history snapshot.
type Order struct {
	// OrderID is the broker-issued order identifier.
	OrderID string
	// Code is the issue code.
	Code string
	// Side is the order direction.
	Side Side
	// Type is the order execution type.
	Type OrderType
	// Quantity is the original order quantity.
	Quantity int64
	// FilledQty is the filled quantity.
	FilledQty int64
	// RemainQty is the remaining quantity.
	RemainQty int64
	// Price is the order price.
	Price int64
	// AvgFillPrice is the average fill price.
	AvgFillPrice int64
	// Status is the current order status.
	Status OrderStatus
	// CreatedAt is the broker or local creation timestamp.
	CreatedAt time.Time
	// UpdatedAt is the latest update timestamp.
	UpdatedAt time.Time
	// Raw keeps the vendor-native source DTO.
	Raw any
}

// Balance stores a common account balance snapshot.
type Balance struct {
	// AccountID is the logical application account identifier.
	AccountID string
	// TotalAsset is the total evaluated asset amount.
	TotalAsset int64
	// CashBalance is the available cash balance.
	CashBalance int64
	// Holdings stores the current holdings.
	Holdings []Holding
	// UpdatedAt is the local snapshot timestamp.
	UpdatedAt time.Time
	// Raw keeps the vendor-native source DTO.
	Raw any
}

// Holding stores one common holding snapshot.
type Holding struct {
	// Code is the issue code.
	Code string
	// Name is the issue name.
	Name string
	// Quantity is the held quantity.
	Quantity int64
	// AvgCost is the average acquisition cost.
	AvgCost int64
	// CurrentPrice is the current price.
	CurrentPrice int64
	// PnL is the evaluated profit and loss.
	PnL int64
	// PnLRate is the fixed-point profit and loss rate.
	PnLRate int64
	// Raw keeps the vendor-native source DTO.
	Raw any
}
