package cutover

import (
	"regexp"
	"sort"
)

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sqlIdentifierPattern constrains user-provided SQL identifiers (outbox
// table and identity column names) so they can be embedded safely into DDL
// and queries that cannot use bind parameters.
var sqlIdentifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validSQLIdentifier(name string) bool {
	return sqlIdentifierPattern.MatchString(name)
}
