package kiwoom

const (
	// BaseURLDefault is the default Kiwoom REST endpoint.
	BaseURLDefault = "https://api.kiwoom.com"
	// MockBaseURLDefault is the default Kiwoom mock REST endpoint.
	MockBaseURLDefault = "https://mockapi.kiwoom.com"
)

const (
	// PathTokenIssue is the OAuth token issue path.
	PathTokenIssue = "/oauth2/token"
)

// BrokerName is the logical Kiwoom broker identifier exposed to callers.
const BrokerName = "kiwoom"

// Config holds the native Kiwoom broker configuration shared by API-level builders.
type Config struct {
	// AccountID is the caller-defined logical account identifier used in logs and lifecycle events.
	AccountID string
	// AppKey is the Kiwoom REST API application key used for OAuth token issuance.
	AppKey string
	// SecretKey is the Kiwoom REST API secret key paired with AppKey.
	SecretKey string
	// AccountNo is the Kiwoom account number used by account-scoped REST APIs.
	AccountNo string
	// BaseURL overrides the Kiwoom REST endpoint when callers need mock or custom routing.
	BaseURL string
}

// RequesterName returns the logical requester name derived from the account ID.
func RequesterName(accountID string) string {
	if accountID == "" {
		return BrokerName
	}
	return BrokerName + "-" + accountID
}
