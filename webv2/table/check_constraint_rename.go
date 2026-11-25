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
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

// renameColumnInCheckConstraints rewrites references to column colId in the
// table's check constraint expressions to newName.
//
// A check expression can refer to the column by its current Spanner name or by
// its source name (for example after an earlier rename, or when the source name
// had to be fixed for Spanner), so both are matched. The source name is skipped
// when another column of the table now has it as its Spanner name, so a rename
// never points a constraint at a different column. Some sources, such as Oracle,
// store the expression with the case the user typed (`qty > 0` for column QTY),
// so names are matched case-insensitively unless another column of the table has
// a name that differs only by case. String literals ('...') are never rewritten.
func renameColumnInCheckConstraints(conv *internal.Conv, tableId, colId, newName string) {
	sp := conv.SpSchema[tableId]
	if len(sp.CheckConstraints) == 0 {
		return
	}

	var names []string
	seen := map[string]bool{}
	addName := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	addName(sp.ColDefs[colId].Name)
	if srcCol, ok := conv.SrcSchema[tableId].ColDefs[colId]; ok {
		usedByOther := false
		for id, col := range sp.ColDefs {
			if id != colId && strings.EqualFold(col.Name, srcCol.Name) {
				usedByOther = true
				break
			}
		}
		if !usedByOther {
			addName(srcCol.Name)
		}
	}
	if len(names) == 0 {
		return
	}

	caseInsensitive := true
	otherNames := []string{}
	for id, col := range sp.ColDefs {
		if id != colId {
			otherNames = append(otherNames, col.Name)
		}
	}
	for id, col := range conv.SrcSchema[tableId].ColDefs {
		if id != colId {
			otherNames = append(otherNames, col.Name)
		}
	}
	for _, other := range otherNames {
		for _, name := range names {
			if strings.EqualFold(other, name) {
				caseInsensitive = false
			}
		}
	}

	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = regexp.QuoteMeta(name)
	}
	pattern := `\b(?:` + strings.Join(quoted, "|") + `)\b`
	if caseInsensitive {
		pattern = `(?i)` + pattern
	}
	re := regexp.MustCompile(pattern)
	for i := range sp.CheckConstraints {
		sp.CheckConstraints[i].Expr = replaceOutsideStringLiterals(sp.CheckConstraints[i].Expr, re, newName)
	}
	conv.SpSchema[tableId] = sp
}

// stringLiteralRe matches a single-quoted SQL string literal, including '' escapes.
var stringLiteralRe = regexp.MustCompile(`'(?:[^']|'')*'`)

// replaceOutsideStringLiterals applies re only to the parts of expr outside
// single-quoted string literals, so values such as 'active' are left unchanged.
func replaceOutsideStringLiterals(expr string, re *regexp.Regexp, repl string) string {
	var b strings.Builder
	last := 0
	for _, loc := range stringLiteralRe.FindAllStringIndex(expr, -1) {
		b.WriteString(re.ReplaceAllLiteralString(expr[last:loc[0]], repl))
		b.WriteString(expr[loc[0]:loc[1]])
		last = loc[1]
	}
	b.WriteString(re.ReplaceAllLiteralString(expr[last:], repl))
	return b.String()
}
