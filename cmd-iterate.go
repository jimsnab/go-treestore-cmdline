package main

import (
	"encoding/json"

	"github.com/jimsnab/go-treestore"
)

type (
	listArgs struct {
		Pattern treestore.TokenPath `json:"pattern"`
		StartAt *int `json:"start_at"`
		Limit *int `json:"limit"`
	}
)

func fnListKeys(ctx *cmdContext, input []byte, response map[string]any) (err error) {
	var args listArgs
	if err = json.Unmarshal(input, &args); err != nil {
		ctx.l.Debugf("invalid lk args: %s", string(input))
		return
	}

	startAt := 0
	limit := 10000

	if args.StartAt != nil {
		startAt = *args.StartAt
	}

	if args.Limit != nil {
		limit = *args.Limit
	}

	skPattern := treestore.MakeStoreKeyFromPath(args.Pattern)
	keys := ctx.ts.GetMatchingKeys(skPattern, startAt, limit)
	response["keys"] = keys

	return
}