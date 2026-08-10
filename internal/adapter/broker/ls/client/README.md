# LS Client

Higher-level LS Securities client built on top of `ls/api`. It keeps the
LS-native DTOs, but adds local subscription handles and per-caller channels.

## Why This Layer Exists

The low-level `api.Realtime` exposes one shared stream per realtime packet type.
That is ideal for infrastructure code, but awkward for callers that expect one
subscription to have one channel.

`client.API` adds that caller-facing behavior:

- deduplicates underlying LS websocket routes
- fans shared packets out to local subscribers
- gives every subscriber its own receive-only channel
- tears down the remote route when the last local subscriber closes
- forwards websocket and fan-out events through one event channel

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    ls "go-back/internal/adapter/broker/ls"
    lsclient "go-back/internal/adapter/broker/ls/client"
)

func main() error {
    ctx := context.Background()

    c, err := lsclient.New(ls.Config{
        AppKey:    "YOUR_APP_KEY",
        AppSecret: "YOUR_APP_SECRET",
        AccountNo: "YOUR_ACCOUNT_NO",
        IsPaper:   true,
    })
    if err != nil {
        return err
    }

    c.Start(ctx)

    sub, err := c.SubscribeTrade(ctx, "005930")
    if err != nil {
        return err
    }
    defer sub.Close(ctx)

    for msg := range sub.Channel() {
        h := msg.Response.Header
        b := msg.Response.Body
        fmt.Printf("%s %s price=%d volume=%d\n", h.TRCode, h.TRKey, b.Price, b.Volume)
    }

    return nil
}
```

## Public Surface

Constructor:

```go
func New(cfg ls.Config) (*API, error)
```

Compatibility note: `Client` remains as an alias to `API`.

Lifecycle and introspection:

- `Start(ctx)`
- `API() *api.API`
- `Events() <-chan Event`

REST convenience methods:

- `GetPrice(ctx, code)`
- `GetOrderbook(ctx, code)`
- `GetMultiPrices(ctx, codes...)`
- `GetBalance(ctx)`

Realtime subscription methods:

- `SubscribeTrade(ctx, code) (*TradeSubscription, error)`
- `SubscribeQuote(ctx, code) (*QuoteSubscription, error)`
- `SubscribeExecution(ctx, htsID) (*ExecutionSubscription, error)`
- `UnsubscribeTrade(ctx, code) error`
- `UnsubscribeQuote(ctx, code) error`
- `UnsubscribeExecution(ctx, htsID) error`

Subscription handles:

- `Channel()`
- `TRCode()`
- `TRKey()`
- `Close(ctx)`

`Close(ctx)` is idempotent. If the handle is the last local subscriber for a
route, the underlying LS websocket route is also unsubscribed.

## Fan-Out Model

```text
LS websocket route
  |
  v
api.Realtime shared stream
  |
  v
client subscriptionRegistry
  |------ TradeSubscription #1 channel
  |------ TradeSubscription #2 channel
  `------ TradeSubscription #3 channel
```

Multiple local subscribers can share one remote LS route. Subscriber channels are
bounded; if a subscriber does not drain its channel, the client emits
`EventSubscriberChannelFull` instead of blocking the whole fan-out path.

Messages keep the native `RealtimeResponse[T]` and include `ConnID`, so callers
can observe which low-level websocket connection slot delivered a packet without
splitting the public stream by connection.

The underlying realtime service manages websocket connection slots internally:
it opens additional slots as route counts grow, distributes routes in 50-route
rounds, and keeps empty slots briefly for reuse before closing them.

## Events

`Events()` returns both forwarded websocket events and client-local fan-out
events. Examples:

- `EventWSConnected`
- `EventControlResponse`
- `EventRealtimeBodyDecodeFailed`
- `EventWSChannelFull`
- `EventSubscriberChannelFull`

The client does not log internally. Callers decide whether events become logs,
metrics, alerts, retries, or ignored diagnostics.

## Error Model

Sentinel-compatible errors:

- `ErrMissingValue`
- `ErrInvalidIssueCode`
- `ErrSubscriptionNotFound`
- `ErrAlreadySubscribed`
- `ErrTooManyCodes`
- `ErrRealtimeClosed`

Context-bearing errors:

- `MissingValueError`
- `InvalidIssueCodeError`
- `AlreadySubscribedError`
- `SubscriptionNotFoundError`
- `CodeLimitError`
- `OperationError`

Use `errors.Is` for category checks and `errors.As` when you need route or field
metadata.

## When To Use `api` Instead

Use `ls/api` directly when you:

- want shared streams instead of per-subscription channels
- want to build your own router or fan-out layer
- need lower-level websocket transport access through `Conn()`
- want the smallest LS-native API surface

Use `ls/client` when you want a reasonable default caller experience.

## Non-Goals

This package is not:

- a strategy runtime
- an order management engine
- a cross-broker domain abstraction
- a durable event bus

It is a focused LS-native client wrapper for application code that wants local
subscription handles.
