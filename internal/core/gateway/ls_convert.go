package gateway

import (
	"strings"

	lsclient "Custom-HTS/internal/adapter/broker/ls/client"
	"Custom-HTS/internal/core/domain"
)

func (b *LSDomainBroker) convertLSTick(msg lsclient.TradeMessage) domain.TickMsg {
	// body stores the native LS US3 trade payload.
	body := msg.Response.Body
	return domain.TickMsg{
		BrokerID:   b.brokerID,
		AccountID:  b.accountID,
		Code:       body.ShCode,
		Price:      body.Price,
		Volume:     body.TradeVolume,
		AccVolume:  body.Volume,
		Change:     body.Change,
		ChangeRate: int64(body.ChangeRate * float64(lsclient.RateScale)),
		TradeTime:  body.TradeTime,
		ReceivedAt: msg.ReceivedAt,
		Raw:        msg,
	}
}

func (b *LSDomainBroker) convertLSQuote(msg lsclient.QuoteMessage) domain.QuoteMsg {
	// body stores the native LS UH1 quote payload.
	body := msg.Response.Body
	// quote stores the converted domain quote message.
	quote := domain.QuoteMsg{
		BrokerID:   b.brokerID,
		AccountID:  b.accountID,
		Code:       body.ShCode,
		TotalAsk:   body.TotalAskSize,
		TotalBid:   body.TotalBidSize,
		ReceivedAt: msg.ReceivedAt,
		Raw:        msg,
	}
	// askPrices stores native ask prices by level.
	askPrices := [10]int64{
		body.AskPrice1,
		body.AskPrice2,
		body.AskPrice3,
		body.AskPrice4,
		body.AskPrice5,
		body.AskPrice6,
		body.AskPrice7,
		body.AskPrice8,
		body.AskPrice9,
		body.AskPrice10,
	}
	// askSizes stores native ask sizes by level.
	askSizes := [10]int64{
		body.AskSize1,
		body.AskSize2,
		body.AskSize3,
		body.AskSize4,
		body.AskSize5,
		body.AskSize6,
		body.AskSize7,
		body.AskSize8,
		body.AskSize9,
		body.AskSize10,
	}
	// bidPrices stores native bid prices by level.
	bidPrices := [10]int64{
		body.BidPrice1,
		body.BidPrice2,
		body.BidPrice3,
		body.BidPrice4,
		body.BidPrice5,
		body.BidPrice6,
		body.BidPrice7,
		body.BidPrice8,
		body.BidPrice9,
		body.BidPrice10,
	}
	// bidSizes stores native bid sizes by level.
	bidSizes := [10]int64{
		body.BidSize1,
		body.BidSize2,
		body.BidSize3,
		body.BidSize4,
		body.BidSize5,
		body.BidSize6,
		body.BidSize7,
		body.BidSize8,
		body.BidSize9,
		body.BidSize10,
	}
	for idx := 0; idx < 10; idx++ {
		quote.Asks[idx] = domain.PriceLevel{
			Price: askPrices[idx],
			Size:  askSizes[idx],
		}
		quote.Bids[idx] = domain.PriceLevel{
			Price: bidPrices[idx],
			Size:  bidSizes[idx],
		}
	}
	return quote
}

func (b *LSDomainBroker) convertLSExec(msg lsclient.ExecutionMessage) domain.ExecMsg {
	// body stores the native LS SC1 execution payload.
	body := msg.Response.Body
	return domain.ExecMsg{
		BrokerID:   b.brokerID,
		AccountID:  firstNonEmpty(body.AccountNo, b.accountID),
		OrderID:    body.OrderNo,
		Code:       firstNonEmpty(body.ShCode, body.IssueCode),
		Side:       parseSide(firstNonEmpty(body.SideCode, body.OrderSideCode)),
		Price:      body.Price,
		Quantity:   body.Quantity,
		RemainQty:  body.RemainQty,
		TotalQty:   body.OrderQty,
		Status:     body.Status,
		ReceivedAt: msg.ReceivedAt,
		Raw:        msg,
	}
}

func parseSide(raw string) domain.Side {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "buy", "b", "2", "02":
		return domain.Buy
	case "sell", "s", "1", "01":
		return domain.Sell
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
