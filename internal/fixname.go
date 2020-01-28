// Copyright 2020 Google LLC
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

package internal

import (
	"regexp"
)

var nameRegexp = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_]*$")
var badFirstChar = regexp.MustCompile("^[^a-zA-Z]")
var badOtherChar = regexp.MustCompile("[^a-zA-Z0-9_]")

// FixName maps a table_name, column_name or index_name into something
// spanner will accept. table_name, column_name or index_name must all
// adhere to the following regexp:
//   {a-z|A-Z}[{a-z|A-Z|0-9|_}+]
// If the first character of the name is not allowed, we replace it by "A".
// We replace all other problem characters by "_".
// Returns a Spanner-acceptable name, and whether we had to change the name.
func FixName(name string) (string, bool) {
	if nameRegexp.MatchString(name) {
		return name, false
	}
	if len(name) == 0 {
		return "BogusEmptyId", true // Don't expect this case.
	}
	name = badFirstChar.ReplaceAllString(name, "A")
	name = badOtherChar.ReplaceAllString(name, "_")
	return name, true
}
