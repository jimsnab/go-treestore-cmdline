package treestore_cmdline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jimsnab/go-cmdline"
	"github.com/jimsnab/go-lane"
)

type (
	cmdDispatcher struct {
		port          int
		iface         string
		tss           *treeStoreSet
		cmdLine       *cmdline.CommandLine
		opLog         OpLogHandler
		reqMu         sync.Mutex
		requestNumber uint64
	}

	OpLogHandler interface {
		OpLogRequest(reqNumber uint64, modify bool, req [][]byte) (err error)
		OpLogResult(reqNumber uint64, modify bool, res []byte) (err error)
	}
)

var writeCommands = map[string]struct{}{}

func (cd *cmdDispatcher) registerWriteCommand(handler cmdline.CommandHandler, specList ...string) {
	parts := strings.Split(specList[0], " ")
	parts = strings.Split(parts[0], "?")
	writeCommands[parts[0]] = struct{}{}

	cd.cmdLine.RegisterCommand(handler, specList...)
}

func newCmdDispatcher(port int, netInterface string, tss *treeStoreSet, opLog OpLogHandler) *cmdDispatcher {
	cd := &cmdDispatcher{
		port:    port,
		iface:   netInterface,
		tss:     tss,
		cmdLine: cmdline.NewCommandLine(),
		opLog:   opLog,
	}

	cd.cmdLine.RegisterCommand(
		fnHelp,
		"help?List the available commands",
	)

	cd.registerWriteCommand(
		fnSetKey,
		"setk <string-key>?Ensures key path is stored (key-escaped), where escaping must escape forward slash as \\s and backslash as \\S.",
	)

	cd.registerWriteCommand(
		fnSetKeyIfExists,
		"setkif <string-testkey> <string-key>?If the test key path exists, ensures key path is stored. Paths are key-escaped, where escaping must escape forward slash as \\s and backslash as \\S.",
	)

	cd.registerWriteCommand(
		fnSetKeyValue,
		"setv <string-key> <string-value>?Sets value (value-escaped) at key path (key-escaped), where value escaping must escape backslash and bytes < 32 or > 127 as hex form \\xx",
		"[--value-type <string-valueType>]?If value is not a byte array, specifies its type (the types that go supports) - string, int, uint, float64, complex128, bool, etc.",
	)

	cd.registerWriteCommand(
		fnSetExStr,
		"setstr <string-key> <string-value>?Convenience function that performs setex of a string value",
		"[--mx]?Must-Exist flag: perform operation only if the value exists",
		"[--nx]?Must-Not-Exist flag: perform operation only if the value doesn't exist",
		"[--sec <string-sec>]?Sets TTL to the Unix epoch seconds (if positive) or relative number of seconds (if negative)",
		"[--ns <string-ns>]?Sets TTL to the Unix epoch nanoseconds (if positive) or relative number of nanoseconds (if negative)",
		"[--relationships <string-relationships>]?Associates a comma-separated list of store addresses with the key; the list can be an empty string",
	)

	cd.registerWriteCommand(
		fnSetExInt,
		"setint <string-key> <int-value>?Convenience function that performs setex of a 32-bit integer",
		"[--mx]?Must-Exist flag: perform operation only if the value exists",
		"[--nx]?Must-Not-Exist flag: perform operation only if the value doesn't exist",
		"[--sec <string-sec>]?Sets TTL to the Unix epoch seconds (if positive) or relative number of seconds (if negative)",
		"[--ns <string-ns>]?Sets TTL to the Unix epoch nanoseconds (if positive) or relative number of nanoseconds (if negative)",
		"[--relationships <string-relationships>]?Associates a comma-separated list of store addresses with the key; the list can be an empty string",
	)

	cd.registerWriteCommand(
		fnSetEx,
		"setex <string-key>?Sets a key path (key-escaped), offering several options",
		"[--value <string-value>]?Sets a value (value-escaped) at the key path; if not specified an existing value is not modified",
		"[--value-type <string-valueType>]?If value is not a byte array, specifies its type (the types that go supports) - string, int, uint, float64, complex128, bool, etc.",
		"[--nil]?Sets the value to nil",
		"[--mx]?Must-Exist flag: perform operation only if the value exists",
		"[--nx]?Must-Not-Exist flag: perform operation only if the value doesn't exist",
		"[--sec <string-sec>]?Sets TTL to the Unix epoch seconds (if positive) or relative number of seconds (if negative)",
		"[--ns <string-ns>]?Sets TTL to the Unix epoch nanoseconds (if positive) or relative number of nanoseconds (if negative)",
		"[--relationships <string-relationships>]?Associates a comma-separated list of store addresses with the key; the list can be an empty string",
	)

	cd.cmdLine.RegisterCommand(
		fnListKeys,
		"lsk <string-pattern>?Lists keys matching the escaped key pattern",
		"[--start <int-start>]?Zero-based starting index, default is 0",
		"[--limit <int-limit>]?Maximum number of keys to return, default is 10000",
		"[--leaves]?List the leaf keys only",
		"[--detailed]?Provide each match with details of the key node such as has_children and relationships, otherwise provide a list of matching key paths",
	)

	cd.cmdLine.RegisterCommand(
		fnKeys,
		"keys <string-pattern>?Lists leaf keys matching the escaped key pattern (alias for lsk --leaves), pattern prefix is removed from the returned list",
		"[--start <int-start>]?Zero-based starting index, default is 0",
		"[--limit <int-limit>]?Maximum number of keys to return, default is 10000",
	)

	cd.registerWriteCommand(
		fnClearKeyMetadata,
		"resetmeta <string-key>?Removes metadata from the specified key",
	)

	cd.registerWriteCommand(
		fnClearMetadataAttribute,
		"delmeta <string-key> <string-attribute>?Removes the specific metadata attribute from the key",
	)

	cd.registerWriteCommand(
		fnDeleteKey,
		"delk <string-key>?Removes the key path, including its data",
	)

	cd.registerWriteCommand(
		fnDeleteKeyWithValue,
		"delv <string-key>?Removes the key path, if it has a value",
		"[--clean]?Removes each parent key node that becomes empty after deletion",
	)

	cd.registerWriteCommand(
		fnDeleteKeyTree,
		"deltree <string-key>?Removes the key path, including its data and children",
	)

	cd.cmdLine.RegisterCommand(
		fnGetKeyTtl,
		"ttlk <string-key>?Gets the Unix epoch timestamp in nanoseconds of when the key will expire, or 0 if it has no expiration",
	)

	cd.cmdLine.RegisterCommand(
		fnGetKeyValue,
		"getv <string-key>?Gets value stored at the specified key path",
	)

	cd.cmdLine.RegisterCommand(
		fnGetKeyValueAtTime,
		"vat <string-key> <string-when>?Gets value stored at the specified key path at the specified Unix nanosecond epoch (absolute timestamp if positive, relative ns if negative)",
	)

	cd.cmdLine.RegisterCommand(
		fnGetKeyValueTtl,
		"ttlv <string-key>?For a key with a value, gets the Unix epoch timestamp in nanoseconds of when the key will expire, or 0 if it has no expiration",
	)

	cd.cmdLine.RegisterCommand(
		fnGetLevelKeys,
		"nodes <string-key> <string-pattern>?Provides the list of key nodes that are children of key",
		"[--start <int-start>]?Zero-based starting index, default is 0",
		"[--limit <int-limit>]?Maximum number of keys to return, default is 10000",
		"[--detailed]?Provide each match with details of the key node such as has_children and relationships, otherwise provide a list of matching key paths",
	)

	cd.cmdLine.RegisterCommand(
		fnListKeyValues,
		"lsv <string-pattern>?List keys that have values and match the specified pattern",
		"[--start <int-start>]?Zero-based starting index, default is 0",
		"[--limit <int-limit>]?Maximum number of keys to return, default is 10000",
		"[--detailed]?Provide each match with details of the key node such as has_children and relationships, otherwise provide a list of matching key paths",
	)

	cd.cmdLine.RegisterCommand(
		fnGetMetadataAttribute,
		"getmeta <string-key> <string-attribute>?Get the metadata attribute value for the key",
	)

	cd.cmdLine.RegisterCommand(
		fnGetMetadataAttributes,
		"lsmeta <string-key>?List the metadata attributes of the key",
	)

	cd.cmdLine.RegisterCommand(
		fnIsKeyIndexed,
		"indexed <string-key>?Indicates if the specified key is indexed (because it has a current value)",
	)

	cd.cmdLine.RegisterCommand(
		fnLocateKey,
		"getk <string-key>?Walks the treestore and returns the key's address",
	)

	cd.registerWriteCommand(
		fnSetKeyTtlSec,
		"expirek <string-key> <string-ttl>?Assigns a new expiration timestamp, in seconds; ttl is the Unix epoch if positive, or relative number of seconds if negative",
	)

	cd.registerWriteCommand(
		fnSetKeyTtlNs,
		"expirekns <string-key> <string-ttl>?Assigns a new expiration timestamp, in nanoseconds; ttl is the Unix epoch if positive, or relative number of seconds if negative",
	)

	cd.registerWriteCommand(
		fnSetKeyValueTtlSec,
		"expirev <string-key> <string-ttl>?Assigns a new expiration timestamp, in seconds, of a key that has a value; ttl is the Unix epoch if positive, or relative number of seconds if negative",
	)

	cd.registerWriteCommand(
		fnSetKeyValueTtlNs,
		"expirevns <string-key> <string-ttl>?Assigns a new expiration timestamp, in nanoseconds, of a key that has a value; ttl is the Unix epoch if positive, or relative number of seconds if negative",
	)

	cd.registerWriteCommand(
		fnSetMetadataAttribute,
		"setmeta <string-key> <string-attribute> <string-value>?Sets or replaces a metadata attribute value for the specified key",
	)

	cd.cmdLine.RegisterCommand(
		fnGetRelationshipValue,
		"follow <string-key> <int-index>?Follows the relationship address at the specified key and index, returning the target key and value",
	)

	cd.cmdLine.RegisterCommand(
		fnKeyFromAddress,
		"addrk <string-address>?Returns the key path for the specified address",
	)

	cd.cmdLine.RegisterCommand(
		fnKeyValueFromAddress,
		"addrv <string-address>?Returns the key value for the specified address",
	)

	cd.cmdLine.RegisterCommand(
		fnExport,
		"export <string-key>?Makes a JSON document from the tree store key",
		"[--base64]?Export the JSON as base64",
	)

	cd.registerWriteCommand(
		fnImport,
		"import <string-key> <string-json>?Loads the specified JSON and stores the data in the tree store",
		"[--base64]?The JSON string is base64",
	)

	cd.cmdLine.RegisterCommand(
		fnGetKeyJson,
		"getjson <string-key>?Returns the key tree in JSON format",
		"[--base64]?The JSON string is base64",
		"[--straskey]?Treat JSON values that are strings as treestore keys",
	)

	cd.registerWriteCommand(
		fnSetKeyJson,
		"setjson <string-key> <string-json>?Creates or replaces the key tree using the JSON data specified",
		"[--base64]?The JSON string is base64",
		"[--straskey]?Treat JSON values that are strings as treestore keys",
	)

	cd.registerWriteCommand(
		fnCreateKeyJson,
		"createjson <string-key> <string-json>?Creates the key tree using the JSON data specified; does not overwrite existing data",
		"[--base64]?The JSON string is base64",
		"[--straskey]?Treat JSON values that are strings as treestore keys",
	)

	cd.registerWriteCommand(
		fnReplaceKeyJson,
		"replacejson <string-key> <string-json>?Replaces the key tree using the JSON data specified; requires existing data",
		"[--base64]?The JSON string is base64",
		"[--straskey]?Treat JSON values that are strings as treestore keys",
	)

	cd.registerWriteCommand(
		fnMergeJson,
		"mergejson <string-key> <string-json>?Overlays the key tree using the JSON data specified into existing data (if any)",
		"[--base64]?The JSON string is base64",
		"[--straskey]?Treat JSON values that are strings as treestore keys",
	)

	cd.registerWriteCommand(
		fnStageKeyJson,
		"stagejson <string-key> <string-json>?Stores the JSON data specified under a unique subkey of the specified key",
		"[--base64]?The JSON string is base64",
		"[--straskey]?Treat JSON values that are strings as treestore keys",
	)

	cd.registerWriteCommand(
		fnCalculateKeyValue,
		"calc <string-key> <string-expression>?Evaluates the specified expression and stores the result value in the specified key",
	)

	cd.registerWriteCommand(
		fnMoveKey,
		"mv <string-src> <string-dest>?Moves the source key to the destination in an atomic operation",
		"[--overwrite]?Overwrite the destination, if it exists",
	)

	cd.registerWriteCommand(
		fnMoveReferencedKey,
		"mvref <string-src> <string-dest>?Moves the source key to the destination in an atomic operation",
		"*[--ref <string-ref>]?Creates or updates a reference key that maintains a relationship to dest (multiple --ref args are supported)",
		"*[--unref <string-unref>]?Removes a source key relationship from a reference key (multiple --unref args are supported)",
		"[--sec <string-sec>]?Assigns dest and ref key TTL to the Unix epoch seconds (if positive) or relative number of seconds (if negative)",
		"[--ns <string-ns>]?Assigns dest and ref key TTL to the Unix epoch nanoseconds (if positive) or relative number of nanoseconds (if negative)",
		"[--overwrite]?Overwrite the destination, if it exists",
	)

	cd.registerWriteCommand(
		fnPurgeDatabase,
		"purge?Discards all the data in the active database",
		"--destructive?Required flag to provide a speed bump on this easy way to lose data",
	)

	cd.registerWriteCommand(
		fnDefineAutoLinkKey,
		"autolink <string-datakey> <string-autolinkkey>?Establishes an auto-link key for a ID-based data key <datakey>, links stored under <autolinkkey>.",
		"*--field <string-fields>?Auto-link paths are made by extracting key segments specified by <fields>. The field's value, obtained from <datakey>/<uniqueid>/<field>, is appended to <autolinkkey>. The <field> subpath can contain * to match any data.",
	)

	cd.registerWriteCommand(
		fnRemoveAutoLinkKey,
		"rmautolink <string-datakey> <string-autolinkkey>?Removes the auto-link key <autolinkkey> from <datakey>, and deletes the links.",
	)

	cd.cmdLine.RegisterCommand(
		fnGetAutoLinkDefinition,
		"getautolink <string-datakey>?Retrieves the auto-link definition stored in <datakey>, if one exists.",
	)

	return cd
}

func (cd *cmdDispatcher) dispatchHandler(l lane.Lane, cs *clientState, req rawRequest) (output []byte, err error) {
	ctx := &cmdContext{
		l:        l,
		response: map[string]any{},
		cd:       cd,
		cs:       cs,
		req:      req,
	}

	ll := l.SetLogLevel(lane.LogLevelError)
	l.SetLogLevel(ll)
	if ll >= lane.LogLevelTrace {
		var printable strings.Builder
		for _, param := range req.exact {
			var sb strings.Builder
			for _, by := range param {
				if by == '\n' {
					sb.WriteString(`\n`)
				} else if by < 32 || by == '\\' || by > 127 {
					sb.WriteString(fmt.Sprintf(`\%02X`, by))
				} else {
					sb.WriteByte(by)
				}
				if sb.Len() > 128 {
					sb.WriteString("…")
					break
				}
			}
			if printable.Len() > 0 {
				printable.WriteString(" ")
			}
			printable.WriteString(sb.String())
		}

		l.Trace(printable.String())
	}

	// ensure unique request number
	reqNumber := uint64(time.Now().UnixNano())
	cd.reqMu.Lock()
	if reqNumber <= cd.requestNumber {
		reqNumber = cd.requestNumber + 1
	}
	cd.requestNumber = reqNumber
	cd.reqMu.Unlock()

	modify := false
	if cd.opLog != nil {
		if len(req.args) > 0 {
			_, modify = writeCommands[req.args[0]]
		}
		cd.opLog.OpLogRequest(reqNumber, modify, req.exact)
	}

	if err = cd.cmdLine.ProcessWithContext(ctx, req.args); err != nil {
		ctx.response["error"] = err.Error()
	}

	// can't use json.Marshal because it imposes some HTML safeguards that are not relevant to json
	buffer := &bytes.Buffer{}
	enc := json.NewEncoder(buffer)
	enc.SetEscapeHTML(false)
	if err = enc.Encode(ctx.response); err != nil {
		l.Errorf("unable to marshal response: %s", err.Error())
		return
	}
	output = bytes.TrimRight(buffer.Bytes(), "\n")

	if cd.opLog != nil {
		if err = cd.opLog.OpLogResult(reqNumber, modify, output); err != nil {
			return
		}
	}

	if ll >= lane.LogLevelTrace {
		l.Tracef("response: %s", string(output))
	}

	return
}
