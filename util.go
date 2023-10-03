package treestore_cmdline

import (
	"strings"

	"github.com/jimsnab/go-treestore"
)

// Converts a token path to a token set, and also translates a
// segment that is exactly an asterisk to a nil segment.
func tokenPathToTokenSetEscapeAsterisk(tokenPath treestore.TokenPath) treestore.TokenSet {
	if tokenPath == "" {
		return treestore.TokenSet{}
	}

	if !strings.HasPrefix(string(tokenPath), "/") {
		tokenPath = "/" + tokenPath
	}

	parts := strings.Split(string(tokenPath[1:]), "/")
	tokens := make(treestore.TokenSet, len(parts))

	for index, part := range parts {
		if part == "*" {
			tokens[index] = nil
		} else {
			tokens[index] = treestore.TokenStringToSegment(part)
		}
	}

	return tokens
}
