# LS Broker Adapter

LS Securities adapter packages for this project. The package is intentionally
split into small layers so callers can choose either native low-level access or
the higher-level subscription wrapper.

## At A Glance

```text
ls/
  config.go          shared LS config, constants, endpoint helpers
  api/               low-level assembled REST + websocket API
    auth/            OAuth token issue/cache service
    rest/            LS REST TR requests and native REST DTOs
    ws/              LS realtime websocket requests, parsing, events, DTOs
    common/error/    API-shared semantic errors
  client/            higher-level local subscription fan-out wrapper
```

## Which Package Should I Use?

| Package | Use When | Owns |
| --- | --- | --- |
| `ls` | You only need shared config and endpoint constants. | `Config`, broker name, default REST/WS URLs |
| `ls/api` | You want native LS REST and realtime streams from one assembled session. | requester, auth, REST, websocket service |
| `ls/client` | You want per-subscription channels and idempotent handles. | local route registry, fan-out, `Close(ctx)` handles |

## Layering

```text
application
  |
  v
ls/client        optional SDK-like layer with local subscriptions
  |
  v
ls/api           low-level LS-native API assembly
  |------ auth    OAuth token lifecycle
  |------ rest    REST TR transport and DTOs
  `------ ws      realtime websocket transport adapter and DTOs
  |
  v
ls               shared LS-native configuration
```

The direction is deliberately one-way. Higher layers compose lower layers; lower
layers do not log, decide application policy, or call back into higher layers.

## Configuration

```go
cfg := ls.Config{
    AccountID: "main",          // optional logical account label
    AppKey:    "YOUR_APP_KEY",
    AppSecret: "YOUR_APP_SECRET",
    AccountNo: "YOUR_ACCOUNT_NO",
    IsPaper:   true,            // paper websocket endpoint when true
}
```

`AccountID` is only a caller-side logical label. LS credentials and account
numbers stay explicit in `Config`.

## Quick Start

Use the higher-level client when you want local subscription channels:

```go
ctx := context.Background()

c, err := client.New(ls.Config{
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
```

Use `api` directly when you want native shared streams and no local fan-out:

```go
a, err := api.New(cfg)
if err != nil {
    return err
}

a.Start(ctx)

if err := a.SubscribeQuote(ctx, "005930"); err != nil {
    return err
}

for msg := range a.Quotes() {
    fmt.Println(msg.Response.Header.TRKey, msg.Response.Body.TotalAskSize)
}
```

## Design Notes

- REST and websocket DTOs are LS-native. Domain conversion belongs above this
  adapter.
- Events are emitted through channels. Low-level packages do not log or print.
- Realtime headers are kept inside `RealtimeResponse[T]` as the source of truth.
- `api.Realtime` owns websocket connection slots internally and keeps one shared
  stream per packet type; messages and events include `ConnID` for observability.
- Realtime route placement opens up to 20 connection slots, fills each round by
  50 routes, allows up to 200 routes per slot, and keeps empty slots briefly for
  reuse before closing them.
- `client` deduplicates remote subscriptions and fans packets out to local
  subscribers.
- `api` embeds `REST` and `Realtime`, so their methods are promoted directly.
- `api.WS` remains as a compatibility alias for existing low-level callers.

## Error Model

Errors are semantic and `errors.Is` / `errors.As` friendly.

- shared API errors live in `api/common/error`
- REST-specific errors live in `api/rest`
- websocket-specific errors live in `api/ws`
- client fan-out errors live in `client`

The adapter returns errors and events upward. Logging, alerting, retries beyond
the provided transport retry policy, and recovery decisions belong to the caller.

## Non-Goals

This package is not:

- a cross-broker abstraction
- a trading engine
- a strategy runtime
- a global event bus
- a persistence layer

It is an LS-native broker adapter that exposes clean lower-level building
blocks for the rest of the application.
