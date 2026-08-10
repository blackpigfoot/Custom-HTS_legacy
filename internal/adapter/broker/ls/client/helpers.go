package client

import (
	"strings"
)

// normalizeIssueCode normalizes an LS issue code into a six-digit stock code.
//
// It accepts both plain six-digit codes and the common "A123456" prefixed form.
func normalizeIssueCode(code string) (string, bool) {
	code = strings.TrimSpace(code)
	if code == "" {
		return "", false
	}

	if after, ok := strings.CutPrefix(code, "A"); ok {
		code = after
	}

	if len(code) != 6 || !isDecimalDigits(code) {
		return "", false
	}
	return code, true
}

// isDecimalDigits reports whether every byte in the string is an ASCII digit.
func isDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
