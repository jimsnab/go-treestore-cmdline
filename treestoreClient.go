package treestore_cmdline

import (
	"time"
)

type (
	rawRequest struct {
		exact [][]byte
		args  []string
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
