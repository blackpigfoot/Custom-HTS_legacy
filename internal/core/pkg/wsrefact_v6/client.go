package wsrefact_v6

import "sync"

// Client serializes caller-visible mux commands, observes mux fatal events, and drives
// the backing mux through the same Stop path on either user request or fatal self-shutdown.
//
// The fatal watcher goroutine exists so the Client owns lifecycle gating in one place:
// once a fatal event arrives it flips stopped before any user goroutine can submit again,
// then routes the failure through the normal Stop path. This lets the backing mux drop
// its own RWMutex-based gating while still guaranteeing that no caller submits after
// shutdown begins.
type Client struct {
	// mux stores the backing mux that owns desired state and worker routing.
	mux *Mux
	// mu serializes Subscribe, Unsubscribe, Stop, and fatal-driven Stop transitions.
	mu sync.Mutex
	// stopped reports that Stop already ran and no more commands should be accepted.
	stopped bool
	// fatalErr captures the deterministic failure cause when the backing mux self-shuts
	// down, exposed to callers via FatalErr after Run returns.
	fatalErr error
	// watchDone signals that the fatal watcher goroutine has cleaned up so Run can join it.
	watchDone chan struct{}
}

// NewClient allocates one caller-facing wrapper and builds its backing mux internally.
func NewClient(config MuxConfig) (*Client, error) {
	mux, err := NewMux(config)
	if err != nil {
		return nil, err
	}
	client := &Client{
		mux:       mux,
		watchDone: make(chan struct{}),
	}
	return client, nil
}

// Run starts the fatal watcher and the backing mux loop, and blocks until both exit.
func (c *Client) Run() {
	go c.watchFatal()
	c.mux.Run()
	<-c.watchDone
}

// Subscribe records one logical subscription request through the backing mux.
func (c *Client) Subscribe(key string) (*Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.submitLocked(userCommandKindSubscribe, key)
}

// Unsubscribe records one logical unsubscribe request through the backing mux.
func (c *Client) Unsubscribe(key string) (*Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.submitLocked(userCommandKindUnsubscribe, key)
}

// Stop permanently prevents new commands and starts backing mux shutdown exactly once.
func (c *Client) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}

	c.stopped = true
	c.mux.stop()
}

// FatalErr returns the deterministic failure cause when the backing mux self-shut down,
// or nil for normal user-initiated shutdown. Safe to call at any time; the value is only
// guaranteed to be final after Run has returned.
func (c *Client) FatalErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fatalErr
}

// watchFatal observes one fatal event from the backing mux and routes it through the
// normal client Stop path.
//
// Reusing Stop keeps lifecycle gating single-source-of-truth: future caller submissions
// are rejected with ErrMuxStopped through exactly the same path as a user-initiated stop.
// The watcher exits when the backing mux closes its fatal channel, which happens both on
// fatal self-shutdown and on normal Stop.
func (c *Client) watchFatal() {
	defer close(c.watchDone)
	event, ok := <-c.mux.FatalCh()
	if !ok {
		return
	}
	c.markFatalAndStop(event.Err)
}

// markFatalAndStop records the fatal cause and drives the backing mux to stop exactly once.
//
// Holding mu across the flag flip but not across mux.Stop avoids holding a user-facing
// lock while waiting on the mux loop, and the duplicate-call guard tolerates the race
// where a user Stop runs concurrently with fatal arrival.
func (c *Client) markFatalAndStop(err error) {
	c.mu.Lock()
	if c.fatalErr == nil {
		c.fatalErr = err
	}
	needStop := !c.stopped
	if needStop {
		c.stopped = true
	}
	c.mu.Unlock()
	if needStop {
		c.mux.stop()
	}
}

// submitLocked enqueues one caller-visible mux command while Client.mu is held.
func (c *Client) submitLocked(kind userCommandKind, key string) (*Subscription, error) {
	if c.stopped {
		return nil, ErrMuxStopped
	}
	return c.mux.submitCommand(kind, key)
}
