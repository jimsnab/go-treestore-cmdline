package main

import (
	"time"
)

type (
	rawRequest struct {
		cmdName string
		input []byte
	}

	clientStateEvent struct {
		newState  cxnState
		eventData any
	}

	TreeStoreClient interface {
		ClientInfo() []string
		MatchFilter(filter map[string]string) bool
		RequestClose()
		IsCloseRequested() bool
		ServerAddr() string
		ClientAddr() string
		ServerNow() time.Time
	}
)

