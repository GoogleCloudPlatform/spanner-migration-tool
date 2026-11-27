// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"regexp"
	"sort"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

// renameColumnInCheckConstraints rewrites references to column colId in the
// table's check constraint expressions to newName.
//
// A check expression can refer to the column by its current Spanner name or by
// its source name (for example after an earlier rename, or when the source name
// had to be fixed for Spanner), so both are matched. Some sources, such as
// Oracle, store the expression with the case the user typed (`qty > 0` for
// column QTY), so names are matched case-insensitively unless another column of
// the table has a name that differs only by case. The source name is skipped
// when another column of the table now has it as its Spanner name, so a rename
// never points a constraint at a different column. String literals are never
// rewritten; which quotes start a literal depends on the Spanner dialect.
func renameColumnInCheckConstraints(conv *internal.Conv, tableId, colId, newName string) {
	sp := conv.SpSchema[tableId]
	if len(sp.CheckConstraints) == 0 {
		return
	}
	srcTable := conv.SrcSchema[tableId]

	candidates := []string{}
	if name := sp.ColDefs[colId].Name; name != "" {
		candidates = append(candidates, name)
	}
	srcName := ""
	if srcCol, ok := srcTable.ColDefs[colId]; ok && srcCol.Name != "" && (len(candidates) == 0 || srcCol.Name != candidates[0]) {
		srcName = srcCol.Name
		candidates = append(candidates, srcName)
	}
	if len(candidates) == 0 {
		return
	}

	// Match case-insensitively unless another column has a name that differs
	// from one of the candidates only by case.
	caseInsensitive := true
	checkOther := func(other string) {
		for _, name := range candidates {
			if strings.EqualFold(other, name) {
				caseInsensitive = false
			}
		}
	}
	for id, col := range sp.ColDefs {
		if id != colId {
			checkOther(col.Name)
		}
	}
	for id, col := range srcTable.ColDefs {
		if id != colId {
			checkOther(col.Name)
		}
	}
	names := candidates
	if srcName != "" {
		for id, col := range sp.ColDefs {
			// Exact match: a match that differs only by case has already made
			// matching case-sensitive above.
			if id != colId && col.Name == srcName {
				// Another column is now called srcName: references to it are
				// references to that column.
				names = candidates[:len(candidates)-1]
				break
			}
		}
	}
	if len(names) == 0 {
		return
	}

	isPg := conv.SpDialect == constants.DIALECT_POSTGRESQL
	for i := range sp.CheckConstraints {
		sp.CheckConstraints[i].Expr = renameInCheckExpr(sp.CheckConstraints[i].Expr, names, newName, caseInsensitive, isPg)
	}
	conv.SpSchema[tableId] = sp
}

var (
	// gsqlStringLiteralRe matches GoogleSQL string literals: '...' and "..."
	// with backslash escapes.
	gsqlStringLiteralRe = regexp.MustCompile(`'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"`)
	// pgStringLiteralRe matches PostgreSQL string literals: '...' with ''
	// escapes, and E'...' that also allows backslash escapes. "..." is a quoted
	// identifier in PostgreSQL, so it is not a literal.
	pgStringLiteralRe = regexp.MustCompile(`[Ee]'(?:\\.|''|[^'\\])*'|'(?:''|[^'])*'`)
)

// renameInCheckExpr replaces whole-name matches of names in expr with newName,
// outside string literals of the Spanner dialect.
func renameInCheckExpr(expr string, names []string, newName string, caseInsensitive, isPg bool) string {
	// Longest name first, so a name is not replaced by a shorter prefix of it.
	sorted := append([]string(nil), names...)
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i]) > len(sorted[j]) })
	quoted := make([]string, len(sorted))
	for i, name := range sorted {
		quoted[i] = regexp.QuoteMeta(name)
	}
	pattern := `(?:` + strings.Join(quoted, "|") + `)`
	if caseInsensitive {
		pattern = `(?i)` + pattern
	}
	nameRe := regexp.MustCompile(pattern)
	literalRe := gsqlStringLiteralRe
	if isPg {
		literalRe = pgStringLiteralRe
	}

	var b strings.Builder
	last := 0
	for _, loc := range literalRe.FindAllStringIndex(expr, -1) {
		b.WriteString(replaceWholeNames(expr[last:loc[0]], nameRe, newName))
		b.WriteString(expr[loc[0]:loc[1]])
		last = loc[1]
	}
	b.WriteString(replaceWholeNames(expr[last:], nameRe, newName))
	return b.String()
}

// replaceWholeNames replaces the matches of nameRe in s that are whole names,
// that is, not preceded or followed by a name character. This works for names
// that start or end with $ or #, such as Oracle PRICE$, where \b does not.
func replaceWholeNames(s string, nameRe *regexp.Regexp, newName string) string {
	var b strings.Builder
	last := 0
	for _, loc := range nameRe.FindAllStringIndex(s, -1) {
		if (loc[0] > 0 && isNameByte(s[loc[0]-1])) || (loc[1] < len(s) && isNameByte(s[loc[1]])) {
			continue
		}
		b.WriteString(s[last:loc[0]])
		b.WriteString(newName)
		last = loc[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

// isNameByte reports whether c can be part of a name. Bytes of multi-byte
// UTF-8 characters are treated as letters.
func isNameByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		c == '_' || c == '$' || c == '#' || c >= 0x80
}
