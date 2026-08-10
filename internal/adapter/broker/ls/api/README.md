# LS API

Low-level LS Securities API assembly. This package builds one shared session
from auth, REST, and realtime websocket services, then exposes LS-native DTOs
and streams without adding local subscription fan-out.

## Features

- OAuth token issuing and caching through `api/auth`
- REST TR calls through embedded `REST`
- realtime websocket subscriptions through embedded `Realtime`
- typed native DTOs for current price, orderbook, balance, trade, quote, and execution packets
- channel-based events instead of internal logging
- semantic errors re-exported from the subpackages

## Architecture

```text
api.API
  |-- auth.Auth          token issue/cache service
  |-- rest.REST          REST TR request/response service
  `-- ws.Realtime       realtime websocket request/parser/event service
      `-- connSlot       one reconnecting websocket connection and its routes
```

`API` embeds `REST` and `Realtime`, so low-level methods are promoted:

```go
a, err := api.New(cfg)
if err != nil {
    return err
}

a.Start(ctx)

price, err := a.GetPrice(ctx, "005930")
if err != nil {
    return err
}

err = a.SubscribeTrade(ctx, "005930")
```

## Package Map

| Path | Role |
| --- | --- |
| `api` | Assembles requester, auth, REST, websocket transport, `REST`, and `Realtime`. |
| `api/auth` | Issues and caches OAuth tokens. |
| `api/rest` | Sends REST TR requests and decodes LS-native REST DTOs. |
| `api/ws` | Sends websocket subscribe/unsubscribe requests, parses realtime packets, emits events. |
| `api/common/error` | Shared semantic error types used by API subpackages. |

## Public Surface

Primary constructor:

```go
func New(cfg ls.Config) (*API, error)
```

Promoted REST methods:

- `GetPrice(ctx, code)`
- `GetOrderbook(ctx, code)`
- `GetMultiPrices(ctx, codes...)`
- `GetBalance(ctx)`

Promoted realtime methods:

- `Start(ctx)`
- `Conn()`
- `Trades()`
- `Quotes()`
- `Executions()`
- `Events()`
- `SubscribeTrade(ctx, code)`
- `SubscribeQuote(ctx, code)`
- `SubscribeExecution(ctx, htsID)`
- `UnsubscribeTrade(ctx, code)`
- `UnsubscribeQuote(ctx, code)`
- `UnsubscribeExecution(ctx, htsID)`

Compatibility note: `Client` remains as an alias to `API`, and `WS` remains as
an alias to `Realtime`.

## REST Example

```go
a, err := api.New(ls.Config{
    AppKey:    "YOUR_APP_KEY",
    AppSecret: "YOUR_APP_SECRET",
    AccountNo: "YOUR_ACCOUNT_NO",
})
if err != nil {
    return err
}

price, err := a.GetPrice(ctx, "005930")
if err != nil {
    return err
}

fmt.Println(price.OutBlock.Hname, price.OutBlock.Price)
```

## Realtime Example

```go
a, err := api.New(cfg)
if err != nil {
    return err
}

a.Start(ctx)

if err := a.SubscribeQuote(ctx, "005930"); err != nil {
    return err
}

for {
    select {
    case msg := <-a.Quotes():
        fmt.Println(msg.Response.Header.TRKey, msg.Response.Body.TotalAskSize)
    case event := <-a.Events():
        if event.Err != nil {
            fmt.Println(event.Kind, event.Err)
        }
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

## Events

The realtime service emits flat events for control responses, decode failures,
transport status, connection slot identity, and channel pressure.

`ConnID` identifies the websocket connection slot that produced an event. The
default placement policy opens up to 20 connection slots, fills each slot in
50-route rounds, allows up to 200 routes per slot, and keeps an empty slot alive
for a short idle TTL so nearby resubscriptions can reuse the same connection.

Important event kinds:

- `EventWSConnected`
- `EventWSReconnected`
- `EventWSDisconnected`
- `EventWSFatal`
- `EventControlResponse`
- `EventRealtimeBodyDecodeFailed`
- `EventUnknownRealtimeTR`
- `EventWSChannelFull`

Events are data. This layer does not log, retry application workflows, or make
recovery decisions.

## Error Model

Common exported errors include:

- `ErrMissingValue`
- `ErrInvalidIssueCode`
- `ErrNilRequester`
- `ErrNilAuth`
- `ErrNilConnection`
- `ErrWSConnectionLimit`
- `ErrRealtimeClosed`
- `ErrT8407CodesRequired`
- `ErrTooManyCodes`
- `ErrInvalidDecimalScale`

Context-bearing error types include:

- `MissingValueError`
- `InvalidIssueCodeError`
- `DecodePathError`
- `FieldParseError`
- `LSError`
- `OperationError`
- `CodeLimitError`

Use `errors.Is` for sentinel checks and `errors.As` for typed context.

## Boundaries

`api` does not provide:

- local per-subscriber channels
- strategy-facing domain conversion
- storage synchronization
- logging or metrics output
- application-level lifecycle orchestration

Those concerns belong above this package.
