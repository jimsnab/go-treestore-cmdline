package treestore_cmdline

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jimsnab/go-cmdline"
	"github.com/jimsnab/go-lane"
	"github.com/jimsnab/go-treestore"
)

type (
	cmdContext struct {
		l        lane.Lane
		response map[string]any
		cd       *cmdDispatcher
		cs       *clientState
		req      rawRequest
	}
)

func fnHelp(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	m := ctx.cd.cmdLine.Summary()
	named := m["named"].([]any)
	help := []map[string][]string{}
	for _, cmd := range named {
		m2 := cmd.(map[string]any)
		primaryMap := m2["primary"].(map[string]string)
		var primary string
		for arg, help := range primaryMap {
			primary = fmt.Sprintf("%s: %s", arg, help)
		}
		options, _ := m2["options"].(map[string]string)

		m3 := map[string][]string{}
		if len(options) == 0 {
			m3[primary] = nil
		} else {
			optStrs := make([]string, 0, len(options))
			for opt, optHelp := range options {
				optStrs = append(optStrs, fmt.Sprintf("%s: %s", opt, optHelp))
			}
			sort.Strings(optStrs)
			m3[primary] = optStrs
		}
		help = append(help, m3)
	}
	sort.Slice(help, func(i, j int) bool {
		var k1, k2 string
		mi := help[i]
		mj := help[j]
		for v := range mi {
			k1 = v
		}
		for v := range mj {
			k2 = v
		}
		return k1 < k2
	})
	ctx.response["help"] = help
	return
}

func fnSetKey(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	address, exists := ctx.cs.ts.SetKey(treestore.MakeStoreKeyFromPath(key))
	ctx.response["address"] = address
	ctx.response["exists"] = exists

	if !exists {
		ctx.cs.tss.dirty.Add(1)
	}

	return
}

func fnSetKeyValue(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	value := ctx.req.exact[2]

	address, firstValue := ctx.cs.ts.SetKeyValue(treestore.MakeStoreKeyFromPath(key), value)
	ctx.response["address"] = address
	ctx.response["firstValue"] = firstValue

	ctx.cs.tss.dirty.Add(1)
	return
}

func bytesToEscapedValue(v []byte) string {
	var sb strings.Builder
	for _, by := range v {
		if by < 32 || by == '\\' {
			sb.WriteString(fmt.Sprintf("\\%02X", by))
		} else {
			sb.WriteByte(by)
		}
	}
	return sb.String()
}

func valueEscape(v any) string {
	switch t := v.(type) {
	case string:
		return bytesToEscapedValue([]byte(t))

	case []byte:
		return bytesToEscapedValue(t)

	default:
		return ""
	}
}

func valueUnescape(v string) []byte {
	unescaped := make([]byte, 0, len(v))

	pos := 0
	for pos < len(v) {
		by := v[pos]
		if by == '\\' && pos+2 < len(v) {
			decoded, err := hex.DecodeString(string(v[pos+1 : pos+3]))
			if err != nil {
				unescaped = append(unescaped, by)
				continue
			}
			by = decoded[0]
			pos += 2
		}
		unescaped = append(unescaped, by)
		pos++
	}

	return unescaped
}

func fnSetEx(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	flags := treestore.SetExFlags(0)
	var value []byte
	if args["--value"].(bool) {
		valArg := args["value"].(string)
		for index, arg := range ctx.req.args {
			if arg == valArg {
				value = ctx.req.exact[index]
				break
			}
		}
	} else {
		flags = flags | treestore.SetExNoValueUpdate
	}

	if args["--mx"].(bool) {
		flags = flags | treestore.SetExMustExist
	} else if args["--nx"].(bool) {
		flags = flags | treestore.SetExMustNotExist
	}

	expireNs := int64(0)
	if args["--sec"].(bool) {
		if expireNs, err = strconv.ParseInt(args["sec"].(string), 10, 64); err != nil {
			return
		}
		expireNs = expireNs * (1000 * 1000 * 1000) // seconds to ns
	} else if args["--ns"].(bool) {
		if expireNs, err = strconv.ParseInt(args["ns"].(string), 10, 64); err != nil {
			return
		}
	}

	var relationships []treestore.StoreAddress
	if args["--relationships"].(bool) {
		list := strings.TrimSpace(args["relationships"].(string))
		if len(list) > 0 {
			parts := strings.Split(list, ",")
			relationships = make([]treestore.StoreAddress, 0, len(parts))
			for _, part := range parts {
				var addr uint64
				if addr, err = strconv.ParseUint(part, 10, 64); err != nil {
					return
				}
				relationships = append(relationships, treestore.StoreAddress(addr))
			}
			ctx.l.Tracef("relationships: %v", relationships)
		}
	}

	address, exists, orgValue := ctx.cs.ts.SetKeyValueEx(
		treestore.MakeStoreKeyFromPath(key),
		value,
		flags,
		expireNs,
		relationships,
	)
	ctx.response["address"] = address
	ctx.response["exists"] = exists

	bytes, valid := orgValue.([]byte)
	if valid {
		ctx.response["orginal_value"] = bytesToEscapedValue(bytes)
	}

	ctx.cs.tss.dirty.Add(1)
	return
}

func fnListKeys(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	pattern := treestore.TokenPath(args["pattern"].(string))

	startAt := 0
	limit := 10000

	if args["--start"].(bool) {
		startAt = args["start"].(int)
	}

	if args["--limit"].(bool) {
		limit = args["limit"].(int)
	}

	skPattern := treestore.MakeStoreKeyFromPath(pattern)
	keys := ctx.cs.ts.GetMatchingKeys(skPattern, startAt, limit)

	if args["--detailed"].(bool) {
		ctx.response["keys"] = keys
	} else {
		keypaths := make([]string, 0, len(keys))
		for _, k := range keys {
			keypaths = append(keypaths, string(k.Key))
		}
		ctx.response["keypaths"] = keypaths
	}

	return
}

func fnClearKeyMetadata(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	ctx.cs.ts.ClearKeyMetdata(treestore.MakeStoreKeyFromPath(key))
	ctx.cs.tss.dirty.Add(1)
	return
}

func fnClearMetadataAttribute(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	attribute := args["attribute"].(string)

	attribExists, orgVal := ctx.cs.ts.ClearMetdataAttribute(treestore.MakeStoreKeyFromPath(key), attribute)

	if attribExists {
		ctx.response["original_value"] = orgVal
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnDeleteKey(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	keyRemoved, valueRemoved, orgVal := ctx.cs.ts.DeleteKey(treestore.MakeStoreKeyFromPath(key))

	ctx.response["key_removed"] = keyRemoved
	if valueRemoved {
		ctx.response["original_value"] = valueEscape(orgVal)
	}
	if keyRemoved {
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnDeleteKeyWithValue(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	clean := args["--clean"].(bool)

	removed, orgVal := ctx.cs.ts.DeleteKeyWithValue(treestore.MakeStoreKeyFromPath(key), clean)

	if removed {
		ctx.response["original_value"] = valueEscape(orgVal)
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnGetKeyTtl(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	ttl := ctx.cs.ts.GetKeyTtl(treestore.MakeStoreKeyFromPath(key))

	ctx.response["ttl"] = fmt.Sprintf("%d", ttl)
	return
}

func fnGetKeyValue(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	val, keyExists, valExists := ctx.cs.ts.GetKeyValue(treestore.MakeStoreKeyFromPath(key))

	ctx.response["key_exists"] = keyExists
	if valExists {
		ctx.response["value"] = valueEscape(val)
	}
	return
}

func fnGetKeyValueAtTime(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	whenStr := args["when"].(string)

	when, err := strconv.ParseInt(whenStr, 10, 64)
	if err != nil {
		return
	}

	val, exists := ctx.cs.ts.GetKeyValueAtTime(treestore.MakeStoreKeyFromPath(key), when)

	if exists {
		ctx.response["value"] = valueEscape(val)
	}
	return
}

func fnGetKeyValueTtl(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	ttl := ctx.cs.ts.GetKeyValueTtl(treestore.MakeStoreKeyFromPath(key))

	ctx.response["ttl"] = fmt.Sprintf("%d", ttl)
	return
}

func fnGetLevelKeys(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	pattern := args["pattern"].(string)

	startAt := 0
	limit := 10000

	if args["--start"].(bool) {
		startAt = args["start"].(int)
	}

	if args["--limit"].(bool) {
		limit = args["limit"].(int)
	}

	keys, count := ctx.cs.ts.GetLevelKeys(treestore.MakeStoreKeyFromPath(key), pattern, startAt, limit)

	ctx.response["count"] = count
	if keys != nil {
		if args["--detailed"].(bool) {
			ctx.response["keys"] = keys
		} else {
			segments := make([]string, 0, len(keys))
			for _, k := range keys {
				segments = append(segments, treestore.EscapeTokenString(string(k.Segment)))
			}
			ctx.response["segments"] = segments
		}
	}

	return
}

func fnListKeyValues(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	pattern := treestore.TokenPath(args["pattern"].(string))

	startAt := 0
	limit := 10000

	if args["--start"].(bool) {
		startAt = args["start"].(int)
	}

	if args["--limit"].(bool) {
		limit = args["limit"].(int)
	}

	skPattern := treestore.MakeStoreKeyFromPath(pattern)
	vals := ctx.cs.ts.GetMatchingKeyValues(skPattern, startAt, limit)

	if args["--detailed"].(bool) {
		ctx.response["values"] = vals
	} else {
		data := make(map[string]string, len(vals))
		for _, v := range vals {
			data[string(v.Key)] = valueEscape(v.CurrentValue)
		}
		ctx.response["key_values"] = data
	}

	return
}

func fnGetMetadataAttribute(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	attribute := args["attribute"].(string)

	attribExists, value := ctx.cs.ts.GetMetadataAttribute(treestore.MakeStoreKeyFromPath(key), attribute)

	if attribExists {
		ctx.response["value"] = value
	}
	return
}

func fnGetMetadataAttributes(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	attributes := ctx.cs.ts.GetMetadataAttributes(treestore.MakeStoreKeyFromPath(key))

	ctx.response["attributes"] = attributes
	return
}

func fnIsKeyIndexed(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	addr, indexed := ctx.cs.ts.IsKeyIndexed(treestore.MakeStoreKeyFromPath(key))
	if indexed {
		ctx.response["address"] = addr
	}
	return
}

func fnLocateKey(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	addr, exists := ctx.cs.ts.LocateKey(treestore.MakeStoreKeyFromPath(key))
	if exists {
		ctx.response["address"] = addr
	}
	return
}

func fnSetKeyTtlSec(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	ttlStr := args["ttl"].(string)
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return
	}
	ttl = ttl * (1000 * 1000 * 1000) // convert seconds to ns

	exists := ctx.cs.ts.SetKeyTtl(treestore.MakeStoreKeyFromPath(key), ttl)
	ctx.response["exists"] = exists

	if exists {
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnSetKeyTtlNs(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	ttlStr := args["ttl"].(string)
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return
	}

	exists := ctx.cs.ts.SetKeyTtl(treestore.MakeStoreKeyFromPath(key), ttl)
	ctx.response["exists"] = exists

	if exists {
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnSetKeyValueTtlSec(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	ttlStr := args["ttl"].(string)
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return
	}
	ttl = ttl * (1000 * 1000 * 1000) // convert seconds to ns

	exists := ctx.cs.ts.SetKeyValueTtl(treestore.MakeStoreKeyFromPath(key), ttl)
	ctx.response["exists"] = exists

	if exists {
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnSetKeyValueTtlNs(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	ttlStr := args["ttl"].(string)
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return
	}

	exists := ctx.cs.ts.SetKeyValueTtl(treestore.MakeStoreKeyFromPath(key), ttl)
	ctx.response["exists"] = exists

	if exists {
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnSetMetadataAttribute(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	attribute := args["attribute"].(string)
	value := args["value"].(string)

	keyExists, priorVal := ctx.cs.ts.SetMetadataAttribute(treestore.MakeStoreKeyFromPath(key), attribute, value)

	ctx.response["key_exists"] = keyExists
	ctx.response["prior_value"] = priorVal
	return
}

func fnGetRelationshipValue(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	index := args["index"].(int)

	hasLink, rv := ctx.cs.ts.GetRelationshipValue(treestore.MakeStoreKeyFromPath(key), index)

	ctx.response["has_link"] = hasLink
	if rv != nil {
		ctx.response["key"] = rv.Sk.Path
		ctx.response["value"] = valueEscape(rv.CurrentValue)
	}
	return
}

func fnKeyFromAddress(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	addressStr := args["address"].(string)
	address, err := strconv.ParseInt(addressStr, 10, 64)
	if err != nil {
		return
	}

	sk, exists := ctx.cs.ts.KeyFromAddress(treestore.StoreAddress(address))

	if exists {
		ctx.response["key"] = sk.Path
	}
	return
}

func fnKeyValueFromAddress(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	addressStr := args["address"].(string)
	address, err := strconv.ParseInt(addressStr, 10, 64)
	if err != nil {
		return
	}

	keyExists, valueExists, sk, val := ctx.cs.ts.KeyValueFromAddress(treestore.StoreAddress(address))

	if keyExists {
		ctx.response["key"] = sk.Path
		if valueExists {
			ctx.response["value"] = valueEscape(val)
		}
	}
	return
}
