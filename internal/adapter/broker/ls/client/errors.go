package client

import "errors"

var (
	// ErrAlreadySubscribed reports that the logical subscription already exists.
	ErrAlreadySubscribed = errors.New("already subscribed")

	// ErrSubscriptionNotFound reports that the logical subscription does not exist.
	ErrSubscriptionNotFound = errors.New("subscription not found")
)

// AlreadySubscribedError reports the duplicate logical subscription key.
type AlreadySubscribedError struct {
	// Kind identifies the logical subscription kind.
	Kind string
	// TRCode is the vendor-native realtime TR code.
	TRCode string
	// TRKey is the vendor-native realtime routing key.
	TRKey string
}

func (e *AlreadySubscribedError) Error() string {
	if e == nil {
		return ""
	}

	msg := "already subscribed"
	if e.Kind != "" {
		msg = "ls " + e.Kind + " already subscribed"
	}
	if e.TRCode != "" || e.TRKey != "" {
		msg += " [" + e.TRCode + ":" + e.TRKey + "]"
	}
	return msg
}

func (e *AlreadySubscribedError) Is(target error) bool {
	return target == ErrAlreadySubscribed
}

// SubscriptionNotFoundError reports the missing logical subscription key.
type SubscriptionNotFoundError struct {
	// Kind identifies the logical subscription kind.
	Kind string
	// TRCode is the vendor-native realtime TR code.
	TRCode string
	// TRKey is the vendor-native realtime routing key.
	TRKey string
}

func (e *SubscriptionNotFoundError) Error() string {
	if e == nil {
		return ""
	}

	msg := "subscription not found"
	if e.Kind != "" {
		msg = "ls " + e.Kind + " subscription not found"
	}
	if e.TRCode != "" || e.TRKey != "" {
		msg += " [" + e.TRCode + ":" + e.TRKey + "]"
	}
	return msg
}

func (e *SubscriptionNotFoundError) Is(target error) bool {
	return target == ErrSubscriptionNotFound
}
