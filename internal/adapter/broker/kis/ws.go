package kis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"Custom-HTS/internal/core/domain"
	"Custom-HTS/internal/core/pkg/subscription"
	ws "Custom-HTS/internal/core/pkg/websocket"
)

// OnMessage — 수신 메시지 파싱 → EventBus 발행.
//
// KIS WS 메시지 형식:
//   - '{' 시작: JSON (구독 응답, 에러, AES 키 수신, PINGPONG)
//   - '|' 포함: 파이프 구분 실시간 데이터 (체결/호가/체결통보)
func (b *Broker) OnMessage(data []byte) {
	// 수신 원문은 verbose 시에만 필요한 디버그 정보 → Debug.
	b.Log.Debug("ws raw message", "data", string(data))

	msg := string(data)
	if strings.HasPrefix(msg, "{") {
		b.handleJSONMessage(data)
		return
	}
	if strings.Contains(msg, "|") {
		b.handlePipeMessage(msg)
		return
	}
	// 알 수 없는 형식은 KIS 프로토콜 변경 가능성 → Warn.
	b.Log.Warn("unknown message format", "data", msg[:min(len(msg), 100)])
}

// Subscribe — Sub.Tr에 따라 내부 TR ID로 분기하여 WS 구독.
// Manager.Subscribe의 lock 안에서 호출됨.
func (b *Broker) Subscribe(ctx context.Context, sub subscription.Sub) error {
	switch sub.Tr {
	case subscription.SubTypeTrade:
		return b.subscribe(ctx, trIDStockTrade, sub.Code)
	case subscription.SubTypeQuote:
		return b.subscribe(ctx, trIDStockQuote, sub.Code)
	case subscription.SubTypeExec:
		return b.subscribe(ctx, trIDStockExec, sub.Code)
	default:
		return fmt.Errorf("unknown SubType: %s", sub.Tr)
	}
}

// Unsubscribe — Sub.Tr에 따라 내부 TR ID로 분기하여 WS 해제.
func (b *Broker) Unsubscribe(ctx context.Context, sub subscription.Sub) error {
	switch sub.Tr {
	case subscription.SubTypeTrade:
		return b.unsubscribe(ctx, trIDStockTrade, sub.Code)
	case subscription.SubTypeQuote:
		return b.unsubscribe(ctx, trIDStockQuote, sub.Code)
	case subscription.SubTypeExec:
		return b.unsubscribe(ctx, trIDStockExec, sub.Code)
	default:
		return fmt.Errorf("unknown SubType: %s", sub.Tr)
	}
}

// RegisterOnReconnect — 추가 재연결 콜백 등록 (SyncService 등).
func (b *Broker) runTransportMessages(ctx context.Context) {
	messageCh := b.conn.Messages()
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-messageCh:
			if !ok {
				return
			}
			b.OnMessage(data)
		}
	}
}

func (b *Broker) runTransportStatus(ctx context.Context) {
	statusCh := b.conn.Status()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-statusCh:
			if !ok {
				return
			}
			switch event.Type {
			case ws.Connected:
				b.OnConnect()
			case ws.Disconnected:
				b.OnDisconnect(event.Err)
			case ws.Reconnecting:
				b.OnReconnecting(event.Attempt)
			case ws.Fatal:
				b.OnFatal(event.Err)
			}
		}
	}
}

// subscribe — WsConnection.Subscribe에 generator 전달.
//
// generator는 cmdCh를 통해 Run goroutine에 전달됨.
// Run goroutine이 subs 저장 + 전송을 순차 처리하므로 중복 구독 불가.
//
// ErrReconnecting: generator가 Run goroutine에 전달됨.
//
//	재연결 후 restoreSubs에서 자동 복원. non-fatal.
//
// ErrNotConnected: Run()이 동작 중이 아님. 상위 조치 필요.
func (b *Broker) subscribe(ctx context.Context, trID string, code string) error {
	key := trID + ":" + code

	subGen := func(ctx context.Context) ([]byte, error) {
		approvalKey, err := b.approvalStore.Get(ctx)
		if err != nil {
			return nil, err
		}
		return b.buildSubPayload(trID, code, approvalKey, wsTypeRegister)
	}

	err := b.conn.Subscribe(ctx, key, subGen)
	if err == nil {
		b.Log.Debug("subscribed", "trID", trID, "code", code)
		return nil
	}

	return fmt.Errorf("subscribe %s %s: %w", trID, code, err)
}

func (b *Broker) unsubscribe(ctx context.Context, trID string, code string) error {
	key := trID + ":" + code

	unsubGen := func(ctx context.Context) ([]byte, error) {
		approvalKey, err := b.approvalStore.Get(ctx)
		if err != nil {
			return nil, err
		}
		return b.buildSubPayload(trID, code, approvalKey, wsTypeUnregister)
	}

	if err := b.conn.Unsubscribe(ctx, key, unsubGen); err != nil {
		return fmt.Errorf("unsubscribe %s %s: %w", trID, code, err)
	}
	return nil
}

func (b *Broker) handleJSONMessage(data []byte) {
	var resp wsRawMessage
	if err := json.Unmarshal(data, &resp); err != nil {
		// JSON 파싱 실패는 프로토콜 문제 가능성 → Error.
		b.Log.Error("json parse failed", "err", err, "raw", string(data))
		return
	}

	// PINGPONG — tr_id로 식별, 즉시 에코.
	if resp.Header.TrID == "PINGPONG" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.conn.Send(ctx, data); err != nil {
			b.Log.Warn("pingpong echo failed", "err", err)
		}
		return
	}

	// AES 암호화키 수신 (연결 직후 서버 1회 전송).
	// 체결통보 복호화의 전제 조건 → Info.
	if resp.Body.Output.Key != "" {
		b.encKey = resp.Body.Output.Key
		b.encIV = resp.Body.Output.IV
		b.Log.Info("encryption key received")
		return
	}

	// 승인키 에러: 다음 재연결에서 fresh 키로 자동 재구독됨.
	if isApprovalKeyError(resp.Body.MsgCode) {
		b.Log.Warn("approval key error", "code", resp.Body.MsgCode)
		return
	}

	if resp.Body.MsgCode != "" {
		// 서버 응답 메시지는 구독 성공/실패 확인용 → Info.
		b.Log.Info("server message", "code", resp.Body.MsgCode, "msg", resp.Body.Msg1)
	}
}

func (b *Broker) handlePipeMessage(msg string) {
	parsed, err := parseRawMessage(msg)
	if err != nil {
		// 1차 파싱(파이프 분리) 실패는 메시지 구조 자체가 깨진 것 → Error.
		b.Log.Error("pipe parse failed", "err", err)
		return
	}

	switch parsed.TrID {
	case trIDStockTrade:
		trades, err := parseStockTrade(parsed.Body)
		if err != nil {
			b.Log.Error("trade parse failed", "err", err)
			b.tradeBreaker.Fail()
			return
		}
		b.tradeBreaker.Success()
		for _, t := range trades {
			b.Channels.SendTick(domain.TickEvent{
				AccountID:  b.AccountID,
				Broker:     b.Broker,
				Code:       t.Code,
				Price:      t.Price,
				Volume:     t.Volume,
				AccVolume:  t.AccVolume,
				Change:     t.Change,
				ChangeRate: t.ChangeRate,
				TradeTime:  t.ExecTime,
				ReceivedAt: time.Now(),
			})
		}

	case trIDStockQuote:
		quote, err := parseStockQuote(parsed.Body)
		if err != nil {
			b.Log.Error("quote parse failed", "err", err)
			b.quoteBreaker.Fail()
			return
		}
		b.quoteBreaker.Success()
		asks := [10]domain.PriceLevel{}
		bids := [10]domain.PriceLevel{}
		for i, a := range quote.Asks {
			asks[i] = domain.PriceLevel{Price: a.Price, Size: a.Size}
		}
		for i, bd := range quote.Bids {
			bids[i] = domain.PriceLevel{Price: bd.Price, Size: bd.Size}
		}
		b.Channels.SendQuote(domain.QuoteEvent{
			AccountID:  b.AccountID,
			Broker:     b.Broker,
			Code:       quote.Code,
			Asks:       asks,
			Bids:       bids,
			TotalAsk:   quote.TotalAsk,
			TotalBid:   quote.TotalBid,
			ReceivedAt: time.Now(),
		})

	case trIDStockExec:
		body := parsed.Body
		if parsed.Encrypted {
			decrypted, err := decryptAES256CBC(body, b.encKey, b.encIV)
			if err != nil {
				// 복호화 실패는 encKey/encIV 유실 가능성 → Error + 서킷브레이커.
				b.Log.Error("exec decrypt failed", "err", err)
				b.decryptBreaker.Fail()
				return
			}
			b.decryptBreaker.Success()
			body = decrypted
		}
		notify, err := parseStockExecNotify(body)
		if err != nil {
			b.Log.Error("exec parse failed", "err", err)
			b.execBreaker.Fail()
			return
		}
		b.execBreaker.Success()
		b.Channels.SendExec(domain.ExecEvent{
			AccountID:  b.AccountID,
			Broker:     b.Broker,
			OrderID:    notify.OrderNo,
			Code:       notify.Code,
			Side:       notify.Side,
			Price:      notify.Price,
			Quantity:   notify.Quantity,
			RemainQty:  notify.RemainQty,
			TotalQty:   notify.TotalQty,
			Status:     notify.Status,
			ExecTime:   notify.ExecTime,
			ReceivedAt: time.Now(),
		})

	default:
		// 알 수 없는 TR_ID는 KIS가 새 데이터 타입을 추가했을 가능성 → Warn.
		b.Log.Warn("unknown TR_ID", "trID", parsed.TrID)
	}
}

func (b *Broker) buildSubPayload(trID, code, approvalKey, trType string) ([]byte, error) {
	return json.Marshal(wsRequest{
		Header: wsRequestHeader{
			ApprovalKey: approvalKey,
			CustType:    "P",
			TrType:      trType,
			ContentType: "utf-8",
		},
		Body: wsRequestBody{
			Input: wsRequestInput{TrID: trID, TrKey: code},
		},
	})
}

func isApprovalKeyError(msgCode string) bool {
	return msgCode == "EGW00123" || msgCode == "EGW00201"
}
