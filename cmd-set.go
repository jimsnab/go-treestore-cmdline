package main

import (
	"encoding/json"

	"github.com/jimsnab/go-treestore"
)

type (
	setKeyArgs struct {
		Key treestore.TokenPath `json:"key"`
	}
)

func fnSetKey(ctx *cmdContext, input []byte, response map[string]any) (err error) {
	var args setKeyArgs
	if err = json.Unmarshal(input, &args); err != nil {
		ctx.l.Debugf("invalid setkey args: %s", string(input))
		return
	}
	
	address, exists := ctx.ts.SetKey(treestore.MakeStoreKeyFromPath(args.Key))
	response["address"] = address
	response["exists"] = exists

	return
}