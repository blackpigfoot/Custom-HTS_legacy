# Websocket Transport

Handler-based reconnecting websocket transport shared by broker packages.

This package is a transport utility. It is not a broker, not a parser, and not
an application event bus.

## Package Role

`WsConnection` is a thin root facade over the layered transport packages. It owns:

- constructor convenience for the layered transport stack
- replayable subscribe or unsubscribe intent handling through its replay layer
- reconnect supervision through its lower-level supervisor
- inbound raw message delivery through `WsHandlers.OnMessage`
- lifecycle delivery through `WsHandlers.OnStatus`

It does not own:

- broker-specific parsing
- store updates
- strategy execution
- app-specific fan-out

## Constructors

- `New(cfg)`
  Preferred constructor. Returns the handler-based transport.

## Inbound Model

`WsHandlers.OnMessage` receives raw websocket payloads synchronously from the
read loop.

Behavior:

- each payload is delivered exactly once
- handlers must return promptly
- slow handlers backpressure the websocket read loop
- the payload slice is valid only until `OnMessage` returns
- copy the payload before storing it or passing it to another goroutine

`WsHandlers.OnStatus` receives transport lifecycle events such as connected,
reconnected, disconnected, reconnecting, and fatal.

## Backpressure Model

Inbound backpressure is synchronous. If a caller needs slower processing, queue
or fan out inside the handler and return quickly. The transport does not choose
the caller's concurrency, buffering, or drop policy.

## Layering

Typical layering:

```text
pkg/websocket.WsConnection
  `- replay.Replay
      |- supervisor.Supervisor
      |   `- session.Session
      `- replay.intentStore
```

For the current LS stack:

- `ls/api` receives handler callbacks through its websocket connection registry
- `ls/client` sits above `ls/api` and adds per-subscription local channels

## Session Model

`WsConnection` separates replay policy, transport supervision, and live sessions.

- `Run(ctx) error` supervises reconnect attempts until the context ends
- `replay.Replay` stores replayable subscribe or unsubscribe intents
- `supervisor.Supervisor` owns reconnect and active-session selection
- each successful dial creates one `session.Session`
- one session owns one websocket conn, one send loop, and one read loop
