package ws

import (
	"encoding/json"
	"time"
)

const (
	// RealtimeTRTrade is the package default LS realtime TR code used for
	// domestic-stock trade ticks.
	RealtimeTRTrade = "US3"
	// RealtimeTRQuote is the package default LS realtime TR code used for
	// domestic-stock orderbook updates.
	RealtimeTRQuote = "UH1"
	// RealtimeTRExecution is the package default LS realtime TR code used for
	// domestic-stock order and execution notifications.
	RealtimeTRExecution = "SC1"
)

// RealtimeHeader is the common realtime header returned with data packets.
type RealtimeHeader struct {
	// TRType is the LS websocket action code when the server includes it.
	TRType string `json:"tr_type"`
	// TRCode is the vendor-native realtime TR code that produced the packet.
	TRCode string `json:"tr_cd"`
	// TRKey is the vendor-native routing key that produced the packet.
	TRKey string `json:"tr_key"`
}

// RealtimeResponse is the reusable LS realtime JSON envelope.
type RealtimeResponse[T any] struct {
	// Header identifies the vendor-native realtime route that emitted the data.
	Header RealtimeHeader `json:"header"`
	// Body contains the TR-specific vendor-native payload.
	Body T `json:"body"`
}

// RealtimeEnvelope is the first-pass LS realtime JSON envelope used for routing.
type RealtimeEnvelope struct {
	// Header identifies the vendor-native realtime route before the body type is known.
	Header RealtimeHeader `json:"header"`
	// Body keeps the raw TR-specific payload until the route selects its DTO type.
	Body json.RawMessage `json:"body"`
}

// TradeBody is the tentative native LS US3 trade-tick body DTO.
type TradeBody struct {
	// ShCode is the short issue code.
	ShCode string `json:"shcode"`
	// Price is the current trade price.
	Price int64 `json:"price,string"`
	// Change is the price change from the previous close.
	Change int64 `json:"change,string"`
	// ChangeRate is the percent change.
	ChangeRate float64 `json:"drate,string"`
	// TradeVolume is the trade volume for this tick.
	TradeVolume int64 `json:"cvolume,string"`
	// Volume is the cumulative session volume.
	Volume int64 `json:"volume,string"`
	// TradeTime is the vendor-native trade time.
	TradeTime string `json:"chetime"`
	// Sign is the vendor-native change sign code.
	Sign string `json:"sign"`
	// Open is the session open price.
	Open int64 `json:"open,string"`
	// High is the session high price.
	High int64 `json:"high,string"`
	// Low is the session low price.
	Low int64 `json:"low,string"`
	// AskPrice is the best ask price included in the tick when present.
	AskPrice int64 `json:"offerho,string"`
	// BidPrice is the best bid price included in the tick when present.
	BidPrice int64 `json:"bidho,string"`
}

// QuoteBody is the tentative native LS UH1 orderbook body DTO.
type QuoteBody struct {
	// ShCode is the short issue code.
	ShCode string `json:"shcode"`
	// QuoteTime is the vendor-native quote time.
	QuoteTime string `json:"hotime"`
	// AskPrice1 is ask price level 1.
	AskPrice1 int64 `json:"offerho1,string"`
	// AskPrice2 is ask price level 2.
	AskPrice2 int64 `json:"offerho2,string"`
	// AskPrice3 is ask price level 3.
	AskPrice3 int64 `json:"offerho3,string"`
	// AskPrice4 is ask price level 4.
	AskPrice4 int64 `json:"offerho4,string"`
	// AskPrice5 is ask price level 5.
	AskPrice5 int64 `json:"offerho5,string"`
	// AskPrice6 is ask price level 6.
	AskPrice6 int64 `json:"offerho6,string"`
	// AskPrice7 is ask price level 7.
	AskPrice7 int64 `json:"offerho7,string"`
	// AskPrice8 is ask price level 8.
	AskPrice8 int64 `json:"offerho8,string"`
	// AskPrice9 is ask price level 9.
	AskPrice9 int64 `json:"offerho9,string"`
	// AskPrice10 is ask price level 10.
	AskPrice10 int64 `json:"offerho10,string"`
	// BidPrice1 is bid price level 1.
	BidPrice1 int64 `json:"bidho1,string"`
	// BidPrice2 is bid price level 2.
	BidPrice2 int64 `json:"bidho2,string"`
	// BidPrice3 is bid price level 3.
	BidPrice3 int64 `json:"bidho3,string"`
	// BidPrice4 is bid price level 4.
	BidPrice4 int64 `json:"bidho4,string"`
	// BidPrice5 is bid price level 5.
	BidPrice5 int64 `json:"bidho5,string"`
	// BidPrice6 is bid price level 6.
	BidPrice6 int64 `json:"bidho6,string"`
	// BidPrice7 is bid price level 7.
	BidPrice7 int64 `json:"bidho7,string"`
	// BidPrice8 is bid price level 8.
	BidPrice8 int64 `json:"bidho8,string"`
	// BidPrice9 is bid price level 9.
	BidPrice9 int64 `json:"bidho9,string"`
	// BidPrice10 is bid price level 10.
	BidPrice10 int64 `json:"bidho10,string"`
	// AskSize1 is ask quantity level 1.
	AskSize1 int64 `json:"offerrem1,string"`
	// AskSize2 is ask quantity level 2.
	AskSize2 int64 `json:"offerrem2,string"`
	// AskSize3 is ask quantity level 3.
	AskSize3 int64 `json:"offerrem3,string"`
	// AskSize4 is ask quantity level 4.
	AskSize4 int64 `json:"offerrem4,string"`
	// AskSize5 is ask quantity level 5.
	AskSize5 int64 `json:"offerrem5,string"`
	// AskSize6 is ask quantity level 6.
	AskSize6 int64 `json:"offerrem6,string"`
	// AskSize7 is ask quantity level 7.
	AskSize7 int64 `json:"offerrem7,string"`
	// AskSize8 is ask quantity level 8.
	AskSize8 int64 `json:"offerrem8,string"`
	// AskSize9 is ask quantity level 9.
	AskSize9 int64 `json:"offerrem9,string"`
	// AskSize10 is ask quantity level 10.
	AskSize10 int64 `json:"offerrem10,string"`
	// BidSize1 is bid quantity level 1.
	BidSize1 int64 `json:"bidrem1,string"`
	// BidSize2 is bid quantity level 2.
	BidSize2 int64 `json:"bidrem2,string"`
	// BidSize3 is bid quantity level 3.
	BidSize3 int64 `json:"bidrem3,string"`
	// BidSize4 is bid quantity level 4.
	BidSize4 int64 `json:"bidrem4,string"`
	// BidSize5 is bid quantity level 5.
	BidSize5 int64 `json:"bidrem5,string"`
	// BidSize6 is bid quantity level 6.
	BidSize6 int64 `json:"bidrem6,string"`
	// BidSize7 is bid quantity level 7.
	BidSize7 int64 `json:"bidrem7,string"`
	// BidSize8 is bid quantity level 8.
	BidSize8 int64 `json:"bidrem8,string"`
	// BidSize9 is bid quantity level 9.
	BidSize9 int64 `json:"bidrem9,string"`
	// BidSize10 is bid quantity level 10.
	BidSize10 int64 `json:"bidrem10,string"`
	// TotalAskSize is the total ask quantity.
	TotalAskSize int64 `json:"offer,string"`
	// TotalBidSize is the total bid quantity.
	TotalBidSize int64 `json:"bid,string"`
}

// ExecutionBody is the tentative native LS SC1 execution body DTO.
type ExecutionBody struct {
	// AccountNo is the vendor account number.
	AccountNo string `json:"accno"`
	// OrderNo is the broker-issued order number.
	OrderNo string `json:"ordno"`
	// ShCode is the short issue code.
	ShCode string `json:"shcode"`
	// IssueCode is an alternate issue code field when present.
	IssueCode string `json:"expcode"`
	// SideCode is the vendor-native buy or sell code.
	SideCode string `json:"medosu"`
	// OrderSideCode is an alternate vendor-native side code.
	OrderSideCode string `json:"buysell"`
	// Price is the execution or order price.
	Price int64 `json:"price,string"`
	// Quantity is the execution quantity.
	Quantity int64 `json:"qty,string"`
	// RemainQty is the remaining order quantity.
	RemainQty int64 `json:"remainqty,string"`
	// OrderQty is the original order quantity.
	OrderQty int64 `json:"ordqty,string"`
	// Status is the vendor-native order status.
	Status string `json:"status"`
	// ExecTime is the vendor-native execution time.
	ExecTime string `json:"chetime"`
}

// TradeMessage is the native LS websocket message delivered by trade subscriptions.
type TradeMessage struct {
	// Response keeps the complete native LS realtime DTO as the header/body source of truth.
	Response RealtimeResponse[TradeBody]
	// ConnID identifies the websocket connection slot that delivered the packet.
	ConnID string
	// ReceivedAt is the local wall-clock time when the websocket payload reached the service.
	ReceivedAt time.Time
}

// QuoteMessage is the native LS websocket message delivered by quote subscriptions.
type QuoteMessage struct {
	// Response keeps the complete native LS realtime DTO as the header/body source of truth.
	Response RealtimeResponse[QuoteBody]
	// ConnID identifies the websocket connection slot that delivered the packet.
	ConnID string
	// ReceivedAt is the local wall-clock time when the websocket payload reached the service.
	ReceivedAt time.Time
}

// ExecutionMessage is the native LS websocket message delivered by execution subscriptions.
type ExecutionMessage struct {
	// Response keeps the complete native LS realtime DTO as the header/body source of truth.
	Response RealtimeResponse[ExecutionBody]
	// ConnID identifies the websocket connection slot that delivered the packet.
	ConnID string
	// ReceivedAt is the local wall-clock time when the websocket payload reached the service.
	ReceivedAt time.Time
}
