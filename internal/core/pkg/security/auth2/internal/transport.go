package internal

import (
	"net/http"
)

func ClientOrDefault(c *http.Client) *http.Client {
    if c != nil {
        return c
    }
    return http.DefaultClient
}