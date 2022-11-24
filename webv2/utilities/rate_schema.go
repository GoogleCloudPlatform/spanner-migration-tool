// Copyright 2022 Google LLC
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

package utilities

func RateSchema(cols, warnings int64, missingPKey bool) string {
	good := func(total, badCount int64) bool { return badCount < total/20 }
	ok := func(total, badCount int64) bool { return badCount < total/2 }
	switch {
	case cols == 0:
		return "GRAY"
	case warnings == 0 && !missingPKey:
		return "GREEN"
	case warnings == 0 && missingPKey:
		return "BLUE"
	case good(cols, warnings) && !missingPKey:
		return "BLUE"
	case good(cols, warnings) && missingPKey:
		return "BLUE"
	case ok(cols, warnings) && !missingPKey:
		return "YELLOW"
	case ok(cols, warnings) && missingPKey:
		return "YELLOW"
	case !missingPKey:
		return "ORANGE"
	default:
		return "ORANGE"
	}
}
