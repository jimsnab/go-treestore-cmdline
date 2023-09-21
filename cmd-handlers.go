package treestore_cmdline

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	levelKey struct {
		Segment     string `json:"segment"`
		HasValue    bool   `json:"has_value"`
		HasChildren bool   `json:"has_children"`
	}

	keyMatchJson struct {
		Key           treestore.TokenPath      `json:"key"`
		Metadata      map[string]string        `json:"metadata,omitempty"`
		HasValue      bool                     `json:"has_value"`
		HasChildren   bool                     `json:"has_children"`
		CurrentValue  string                   `json:"current_value,omitempty"`
		CurrentType   string                   `json:"current_type,omitempty"`
		Relationships []treestore.StoreAddress `json:"relationships,omitempty"`
	}

	keyValueMatchJson struct {
		Key           treestore.TokenPath      `json:"key"`
		Metadata      map[string]string        `json:"metadata,omitempty"`
		HasChildren   bool                     `json:"has_children"`
		CurrentValue  string                   `json:"current_value,omitempty"`
		CurrentType   string                   `json:"current_type,omitempty"`
		Relationships []treestore.StoreAddress `json:"relationships,omitempty"`
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

func fnSetKeyIfExists(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	testkey := treestore.TokenPath(args["testkey"].(string))

	address, exists := ctx.cs.ts.SetKeyIfExists(treestore.MakeStoreKeyFromPath(testkey), treestore.MakeStoreKeyFromPath(key))
	ctx.response["address"] = address
	ctx.response["exists"] = exists

	if !exists {
		ctx.cs.tss.dirty.Add(1)
	}

	return
}

func valueFromCmdLine(ctx *cmdContext, args cmdline.Values, exactIndex int) (val any, err error) {
	value := ctx.req.exact[exactIndex]
	valueType, _ := args["valueType"].(string)

	return cmdLineToNativeValue(value, valueType)
}

func cmdLineToNativeValue(value []byte, valueType string) (val any, err error) {
	switch valueType {
	case "int":
		if len(value) != 4 {
			err = errors.New("invalid int value")
			return
		}
		val = int(binary.BigEndian.Uint32(value))
		return
	case "int8":
		if len(value) != 1 {
			err = errors.New("invalid int8 value")
			return
		}
		val = int8(value[0])
		return
	case "int16":
		if len(value) != 2 {
			err = errors.New("invalid int16 value")
			return
		}
		val = int16(binary.BigEndian.Uint16(value))
		return
	case "int32":
		if len(value) != 4 {
			err = errors.New("invalid int32 value")
			return
		}
		val = int32(binary.BigEndian.Uint32(value))
		return
	case "int64":
		if len(value) != 8 {
			err = errors.New("invalid int64 value")
			return
		}
		val = int64(binary.BigEndian.Uint64(value))
		return
	case "uint":
		if len(value) != 4 {
			err = errors.New("invalid uint value")
			return
		}
		val = binary.BigEndian.Uint32(value)
		return
	case "uint8":
		if len(value) != 1 {
			err = errors.New("invalid uint8 value")
			return
		}
		val = int8(value[0])
		return
	case "uint16":
		if len(value) != 2 {
			err = errors.New("invalid uint16 value")
			return
		}
		val = binary.BigEndian.Uint16(value)
		return
	case "uint32":
		if len(value) != 4 {
			err = errors.New("invalid uint32 value")
			return
		}
		val = binary.BigEndian.Uint32(value)
		return
	case "uint64":
		if len(value) != 8 {
			err = errors.New("invalid uint64 value")
			return
		}
		val = binary.BigEndian.Uint64(value)
		return
	case "float32":
		var f64 float64
		f64, err = strconv.ParseFloat(string(value), 32)
		if err != nil {
			return
		}
		val = float32(f64)
		return
	case "float64":
		val, err = strconv.ParseFloat(string(value), 32)
		if err != nil {
			return
		}
		return
	case "bool":
		val, err = strconv.ParseBool(string(value))
		if err != nil {
			return
		}
		return
	case "complex64":
		var c128 complex128
		c128, err = strconv.ParseComplex(string(value), 64)
		if err != nil {
			return
		}
		val = complex64(c128)
		return
	case "complex128":
		val, err = strconv.ParseComplex(string(value), 128)
		if err != nil {
			return
		}
		return
	case "string":
		val = string(value)
		return
	case "":
		val = value
		return
	}

	if strings.HasPrefix(valueType, "json-") {
		val = value
		return
	}

	err = errors.New("unrecognized value type " + valueType)
	return
}

func fnSetKeyValue(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	value, err := valueFromCmdLine(ctx, args, 2)
	if err != nil {
		return
	}

	address, firstValue := ctx.cs.ts.SetKeyValue(treestore.MakeStoreKeyFromPath(key), value)
	ctx.response["address"] = address
	ctx.response["firstValue"] = firstValue

	ctx.cs.tss.dirty.Add(1)
	return
}

func bytesToEscapedValue(v []byte) string {
	if v == nil {
		return ""
	}
	var sb strings.Builder
	for _, by := range v {
		if by < 32 || by == '\\' || by > 127 {
			sb.WriteString(fmt.Sprintf("\\%02X", by))
		} else {
			sb.WriteByte(by)
		}
	}
	return sb.String()
}

func valueUnescape(v string) []byte {
	unescaped := make([]byte, 0, len(v))

	pos := 0
	for pos < len(v) {
		by := v[pos]
		if by == '\\' && pos+2 < len(v) {
			decoded, err := hex.DecodeString(string(v[pos+1 : pos+3]))
			if err == nil {
				by = decoded[0]
				pos += 2
			}
		}
		unescaped = append(unescaped, by)
		pos++
	}

	return unescaped
}

func setEx(args cmdline.Values, ctx *cmdContext, value any, flags treestore.SetExFlags) (err error) {
	key := treestore.TokenPath(args["key"].(string))

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

	if orgValue != nil {
		if err = addValueToResponse(ctx, orgValue, "original"); err != nil {
			return
		}
	}

	ctx.cs.tss.dirty.Add(1)
	return
}

func fnSetExStr(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	value := treestore.TokenPath(args["value"].(string))

	return setEx(args, ctx, value, 0)
}

func fnSetExInt(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	value := args["value"].(int)

	return setEx(args, ctx, value, 0)
}

func fnSetEx(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)

	flags := treestore.SetExFlags(0)
	var value any
	if args["--value"].(bool) {
		if args["--nil"].(bool) {
			err = fmt.Errorf("--value and --nil are mutually exclusive")
			return
		}
		valArg := args["value"].(string)
		for index, arg := range ctx.req.args {
			if arg == valArg {
				value, err = valueFromCmdLine(ctx, args, index)
				if err != nil {
					return
				}
				break
			}
		}
	} else if !args["--nil"].(bool) {
		flags = flags | treestore.SetExNoValueUpdate
	}

	return setEx(args, ctx, value, flags)
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
		kmj := make([]*keyMatchJson, 0, len(keys))
		for _, key := range keys {
			km := keyMatchJson{
				Key:           key.Key,
				Metadata:      key.Metadata,
				HasValue:      key.HasValue,
				HasChildren:   key.HasChildren,
				Relationships: key.Relationships,
			}

			var v, t string
			if v, t, err = nativeValueToCmdLine(key.CurrentValue); err != nil {
				return
			}
			km.CurrentValue = v
			km.CurrentType = t
			kmj = append(kmj, &km)
		}
		ctx.response["keys"] = kmj
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

	ctx.cs.ts.ClearKeyMetadata(treestore.MakeStoreKeyFromPath(key))
	ctx.cs.tss.dirty.Add(1)
	return
}

func fnClearMetadataAttribute(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	attribute := args["attribute"].(string)

	attribExists, orgVal := ctx.cs.ts.ClearMetadataAttribute(treestore.MakeStoreKeyFromPath(key), attribute)

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
		if err = addValueToResponse(ctx, orgVal, "original"); err != nil {
			return
		}
		ctx.cs.tss.dirty.Add(1)
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
		if err = addValueToResponse(ctx, orgVal, "original"); err != nil {
			return
		}
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnDeleteKeyTree(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	removed := ctx.cs.ts.DeleteKeyTree(treestore.MakeStoreKeyFromPath(key))

	ctx.response["removed"] = removed
	if removed {
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnGetKeyTtl(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	ttl := ctx.cs.ts.GetKeyTtl(treestore.MakeStoreKeyFromPath(key))

	if ttl > 0 {
		ctx.response["ttl"] = fmt.Sprintf("%d", ttl)
	}
	return
}

func addValueToResponse(ctx *cmdContext, val any, prefix string) (err error) {
	var vk, vt string
	if prefix != "" {
		vk = prefix + "_value"
		vt = prefix + "_type"
	} else {
		vk = "value"
		vt = "type"
	}

	ev, et, err := nativeValueToCmdLine(val)
	if err != nil {
		return
	}

	ctx.response[vk] = ev
	ctx.response[vt] = et
	return
}

func nativeValueToCmdLine(val any) (encodedVal, encodedType string, err error) {
	switch t := val.(type) {
	case []byte:
		encodedVal = bytesToEscapedValue(t)

	case string:
		encodedVal = bytesToEscapedValue([]byte(t))
		encodedType = "string"

	case int:
		by := make([]byte, 4)
		binary.BigEndian.PutUint32(by, uint32(t))
		encodedVal = bytesToEscapedValue(by)
		encodedType = "int"
	case int8:
		by := []byte{byte(t)}
		encodedVal = bytesToEscapedValue(by)
		encodedType = "int8"
	case int16:
		by := make([]byte, 2)
		binary.BigEndian.PutUint16(by, uint16(t))
		encodedVal = bytesToEscapedValue(by)
		encodedType = "int16"
	case int32:
		by := make([]byte, 4)
		binary.BigEndian.PutUint32(by, uint32(t))
		encodedVal = bytesToEscapedValue(by)
		encodedType = "int32"
	case int64:
		by := make([]byte, 8)
		binary.BigEndian.PutUint64(by, uint64(t))
		encodedVal = bytesToEscapedValue(by)
		encodedType = "int64"

	case uint:
		by := make([]byte, 4)
		binary.BigEndian.PutUint32(by, uint32(t))
		encodedVal = bytesToEscapedValue(by)
		encodedType = "uint"
	case uint8:
		by := []byte{byte(t)}
		encodedVal = bytesToEscapedValue(by)
		encodedType = "uint8"
	case uint16:
		by := make([]byte, 2)
		binary.BigEndian.PutUint16(by, uint16(t))
		encodedVal = bytesToEscapedValue(by)
		encodedType = "uint16"
	case uint32:
		by := make([]byte, 4)
		binary.BigEndian.PutUint32(by, uint32(t))
		encodedVal = bytesToEscapedValue(by)
		encodedType = "uint32"
	case uint64:
		by := make([]byte, 8)
		binary.BigEndian.PutUint64(by, uint64(t))
		encodedVal = bytesToEscapedValue(by)
		encodedType = "uint64"

	case float32, float64, bool, complex64, complex128:
		str := fmt.Sprintf("%v", t)
		encodedVal = bytesToEscapedValue([]byte(str))
		encodedType = fmt.Sprintf("%T", t)

	case nil:
		encodedType = "nil"

	default:
		var by []byte
		by, err = json.Marshal(t)
		if err != nil {
			return
		}
		encodedVal = bytesToEscapedValue(by)
		encodedType = fmt.Sprintf("json-%T", t)
	}
	return
}

func fnGetKeyValue(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	val, keyExists, valExists := ctx.cs.ts.GetKeyValue(treestore.MakeStoreKeyFromPath(key))

	ctx.response["key_exists"] = keyExists
	if valExists {
		if err = addValueToResponse(ctx, val, ""); err != nil {
			return
		}
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
		if err = addValueToResponse(ctx, val, ""); err != nil {
			return
		}
	}
	return
}

func fnGetKeyValueTtl(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	ttl := ctx.cs.ts.GetKeyValueTtl(treestore.MakeStoreKeyFromPath(key))

	if ttl > 0 {
		ctx.response["ttl"] = fmt.Sprintf("%d", ttl)
	}
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

	keys := ctx.cs.ts.GetLevelKeys(treestore.MakeStoreKeyFromPath(key), pattern, startAt, limit)

	if keys != nil {
		if args["--detailed"].(bool) {
			wireKeys := make([]levelKey, 0, len(keys))
			for _, k := range keys {
				wk := levelKey{
					Segment:     treestore.TokenSegmentToString(k.Segment),
					HasChildren: k.HasChildren,
					HasValue:    k.HasValue,
				}
				wireKeys = append(wireKeys, wk)
			}
			ctx.response["keys"] = wireKeys
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
		// value-escape the value
		jsonVals := make([]*keyValueMatchJson, 0, len(vals))
		for _, val := range vals {
			kvm := keyValueMatchJson{
				Key:           val.Key,
				Metadata:      val.Metadata,
				HasChildren:   val.HasChildren,
				Relationships: val.Relationships,
			}
			var v, t string
			if v, t, err = nativeValueToCmdLine(val.CurrentValue); err != nil {
				return
			}
			kvm.CurrentValue = v
			kvm.CurrentType = t
			jsonVals = append(jsonVals, &kvm)
		}
		ctx.response["values"] = jsonVals
	} else {
		data := make(map[string]string, len(vals))
		for _, val := range vals {
			var jsonVal string
			if val.CurrentValue != nil {
				if jsonVal, _, err = nativeValueToCmdLine(val.CurrentValue); err != nil {
					return
				}
			}
			data[string(val.Key)] = jsonVal
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

	ctx.cs.tss.dirty.Add(1)
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

		if rv.CurrentValue != nil {
			if err = addValueToResponse(ctx, rv.CurrentValue, ""); err != nil {
				return
			}
		}
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
			if err = addValueToResponse(ctx, val, ""); err != nil {
				return
			}
		}
	}
	return
}

func fnExport(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	jsonData, err := ctx.cs.ts.Export(treestore.MakeStoreKeyFromPath(key))
	if err != nil {
		return
	}

	if args["--base64"].(bool) {
		ctx.response["base64"] = base64.StdEncoding.EncodeToString(jsonData)
	} else {
		var payload any
		if err = json.Unmarshal(jsonData, &payload); err != nil {
			return
		}

		ctx.response["data"] = payload
	}

	return
}

func fnImport(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	var jsonData []byte
	if args["--base64"].(bool) {
		if jsonData, err = base64.StdEncoding.DecodeString(args["json"].(string)); err != nil {
			return
		}
	} else {
		jsonData = []byte(args["json"].(string))
	}

	err = ctx.cs.ts.Import(treestore.MakeStoreKeyFromPath(key), []byte(jsonData))
	if err != nil {
		return
	}

	ctx.cs.tss.dirty.Add(1)
	return
}

func fnGetKeyJson(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	opts := treestore.JsonOptions(0)
	if args["--straskey"].(bool) {
		opts = treestore.JsonStringValuesAsKeys
	}

	jsonData, err := ctx.cs.ts.GetKeyAsJson(treestore.MakeStoreKeyFromPath(key), opts)
	if err != nil {
		return
	}

	if args["--base64"].(bool) {
		ctx.response["base64"] = base64.StdEncoding.EncodeToString(jsonData)
	} else {
		var payload any
		if err = json.Unmarshal(jsonData, &payload); err != nil {
			return
		}

		ctx.response["data"] = payload
	}

	return
}

func fnSetKeyJson(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	var jsonData []byte
	if args["--base64"].(bool) {
		if jsonData, err = base64.StdEncoding.DecodeString(args["json"].(string)); err != nil {
			return
		}
	} else {
		jsonData = []byte(args["json"].(string))
	}

	opts := treestore.JsonOptions(0)
	if args["--straskey"].(bool) {
		opts = treestore.JsonStringValuesAsKeys
	}

	replaced, addr, err := ctx.cs.ts.SetKeyJson(treestore.MakeStoreKeyFromPath(key), []byte(jsonData), opts)
	if err != nil {
		return
	}

	ctx.response["replaced"] = replaced
	ctx.response["address"] = addr
	ctx.cs.tss.dirty.Add(1)
	return
}

func fnCreateKeyJson(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	var jsonData []byte
	if args["--base64"].(bool) {
		if jsonData, err = base64.StdEncoding.DecodeString(args["json"].(string)); err != nil {
			return
		}
	} else {
		jsonData = []byte(args["json"].(string))
	}

	opts := treestore.JsonOptions(0)
	if args["--straskey"].(bool) {
		opts = treestore.JsonStringValuesAsKeys
	}

	created, addr, err := ctx.cs.ts.CreateKeyJson(treestore.MakeStoreKeyFromPath(key), []byte(jsonData), opts)
	if err != nil {
		return
	}

	if created {
		ctx.response["address"] = addr
	}
	ctx.cs.tss.dirty.Add(1)
	return
}

func fnReplaceKeyJson(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	var jsonData []byte
	if args["--base64"].(bool) {
		if jsonData, err = base64.StdEncoding.DecodeString(args["json"].(string)); err != nil {
			return
		}
	} else {
		jsonData = []byte(args["json"].(string))
	}

	opts := treestore.JsonOptions(0)
	if args["--straskey"].(bool) {
		opts = treestore.JsonStringValuesAsKeys
	}

	replaced, addr, err := ctx.cs.ts.ReplaceKeyJson(treestore.MakeStoreKeyFromPath(key), []byte(jsonData), opts)
	if err != nil {
		return
	}

	if replaced {
		ctx.response["address"] = addr
	}
	ctx.cs.tss.dirty.Add(1)
	return
}

func fnMergeJson(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	var jsonData []byte
	if args["--base64"].(bool) {
		if jsonData, err = base64.StdEncoding.DecodeString(args["json"].(string)); err != nil {
			return
		}
	} else {
		jsonData = []byte(args["json"].(string))
	}

	opts := treestore.JsonOptions(0)
	if args["--straskey"].(bool) {
		opts = treestore.JsonStringValuesAsKeys
	}

	addr, err := ctx.cs.ts.MergeKeyJson(treestore.MakeStoreKeyFromPath(key), []byte(jsonData), opts)
	if err != nil {
		return
	}

	ctx.response["address"] = addr
	ctx.cs.tss.dirty.Add(1)
	return
}

func fnCalculateKeyValue(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))
	expression := args["expression"].(string)

	address, newVal := ctx.cs.ts.CalculateKeyValue(treestore.MakeStoreKeyFromPath(key), expression)
	if newVal != nil {
		ctx.response["address"] = address
		addValueToResponse(ctx, newVal, "")
		ctx.cs.tss.dirty.Add(1)
	}
	return
}

func fnStageKeyJson(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	key := treestore.TokenPath(args["key"].(string))

	var jsonData []byte
	if args["--base64"].(bool) {
		if jsonData, err = base64.StdEncoding.DecodeString(args["json"].(string)); err != nil {
			return
		}
	} else {
		jsonData = []byte(args["json"].(string))
	}

	opts := treestore.JsonOptions(0)
	if args["--straskey"].(bool) {
		opts = treestore.JsonStringValuesAsKeys
	}

	tempSk, addr, err := ctx.cs.ts.StageKeyJson(treestore.MakeStoreKeyFromPath(key), []byte(jsonData), opts)
	if err != nil {
		return
	}

	ctx.response["tempkey"] = tempSk.Path
	ctx.response["address"] = addr
	ctx.cs.tss.dirty.Add(1)
	return
}

func fnMoveKey(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	sk := treestore.TokenPath(args["src"].(string))
	dk := treestore.TokenPath(args["dest"].(string))
	oflag := args["--overwrite"].(bool)

	exists, moved := ctx.cs.ts.MoveKey(treestore.MakeStoreKeyFromPath(sk), treestore.MakeStoreKeyFromPath(dk), oflag)
	if err != nil {
		return
	}

	ctx.response["exists"] = exists
	ctx.response["moved"] = moved
	ctx.cs.tss.dirty.Add(1)
	return
}

func fnMoveReferencedKey(args cmdline.Values) (err error) {
	ctx := args[""].(*cmdContext)
	sk := treestore.TokenPath(args["src"].(string))
	dk := treestore.TokenPath(args["dest"].(string))
	oflag := args["--overwrite"].(bool)

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

	refArgs, specified := args["ref"].([]string)
	if !specified {
		refArgs = []string{}
	}
	refs := make([]treestore.StoreKey, 0, len(refArgs))

	for _, ref := range refArgs {
		refs = append(refs, treestore.MakeStoreKeyFromPath(treestore.TokenPath(ref)))
	}

	unrefArgs, specified := args["unref"].([]string)
	if !specified {
		unrefArgs = []string{}
	}
	unrefs := make([]treestore.StoreKey, 0, len(unrefArgs))

	for _, unref := range unrefArgs {
		unrefs = append(unrefs, treestore.MakeStoreKeyFromPath(treestore.TokenPath(unref)))
	}

	exists, moved := ctx.cs.ts.MoveReferencedKey(treestore.MakeStoreKeyFromPath(sk), treestore.MakeStoreKeyFromPath(dk), oflag, expireNs, refs, unrefs)
	if err != nil {
		return
	}

	ctx.response["exists"] = exists
	ctx.response["moved"] = moved
	ctx.cs.tss.dirty.Add(1)
	return
}
