package main

import (
	"encoding/json"
	"fmt"

	"github.com/jimsnab/go-lane"
	"github.com/jimsnab/go-treestore"
)

type (
	cmdContext struct {
		l        lane.Lane
		cmdName  string
		tss       *treeStoreSet
		ts        *treestore.TreeStore
	}
	cmdHandler func(ctx *cmdContext, input []byte, response map[string]any) error

	cmdDispatcher struct {
		port      int
		iface     string
		tss       *treeStoreSet
		handlers  map[string]cmdHandler
	}
)

var handlerTable = map[string]cmdHandler{
	"setk": fnSetKey,
	"lsk": fnListKeys,
}

func newCmdDispatcher(port int, netInterface string, tss *treeStoreSet) *cmdDispatcher {
	cd := &cmdDispatcher{
		port:      port,
		iface:     netInterface,
		tss: tss,
		handlers:  handlerTable,
	}

	return cd
}

func (cd *cmdDispatcher) dispatchHandler(l lane.Lane, cmdName string, input []byte) (output []byte, err error) {
	var response map[string]any

	handler := cd.handlers[cmdName]
	if handler == nil {
		l.Debugf("unsupported command '%s' rejected", cmdName)
		response = map[string]any{
			"error": fmt.Sprintf("unsupported command '%s'", cmdName),
		}
	} else {
		ctx := &cmdContext{
			l: l,
			cmdName: cmdName,
			tss: cd.tss,
		}
		response = map[string]any{}
		err = handler(ctx, input, response)
		if err != nil {
			l.Errorf("handler '%s' error: %s", cmdName, err.Error())
			return
		}
	}

	output, err = json.Marshal(response)
	if err != nil {
		l.Errorf("unable to marshal response: %s", err.Error())
		return
	}
	l.Tracef("response: %s", string(output))
	return
}
