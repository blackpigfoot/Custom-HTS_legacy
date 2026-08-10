package main

import (
	"log"

	"Custom-HTS/internal/adapter/broker/ls/test/component"
)

// main is the local LS smoke-test entrypoint.
func main() {
	if err := component.RunLocalTest(); err != nil {
		log.Printf("ls api local test failed: %v", err)
	}
}
