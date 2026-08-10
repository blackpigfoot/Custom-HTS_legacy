package ls

const (
	// BaseURLDefault is the default LS REST endpoint.
	BaseURLDefault = "https://openapi.ls-sec.co.kr:8080"
)

const (
	// PathTokenIssue is the OAuth token issue path.
	PathTokenIssue = "/oauth2/token"
)

// BrokerName is the logical LS broker identifier exposed to callers.
const BrokerName = "ls"

// Config holds the native LS broker configuration shared by API-level builders.
type Config struct {
	// AccountID is the caller-defined logical account identifier used in logs
	// and lifecycle events. It is optional but strongly recommended when the
	// application manages more than one LS account.
	AccountID string
	// AppKey is the LS OpenAPI application key used for OAuth token issuance.
	AppKey string
	// AppSecret is the LS OpenAPI application secret paired with AppKey.
	AppSecret string
	// AccountNo is the LS account number used by account-scoped REST APIs.
	AccountNo string
}

// RequesterName returns the logical requester name derived from the account ID.
func RequesterName(accountID string) string {
	if accountID == "" {
		return BrokerName
	}
	return BrokerName + "-" + accountID
}
