/*
	Copyright 2025 Google LLC

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
*/
package utils

import "strings"

func SanitizeCsvRow(s *string) string {
	if s == nil {
		return ""
	}
	*s = strings.ReplaceAll(*s, "\t", " ")
	*s = strings.ReplaceAll(*s, "\n", " ")

	return *s
}

func JoinString(items *[]string, defaultValue string) string {
	if items == nil || len(*items) == 0 {
		return defaultValue
	}
	return strings.Join(*items, ",")
}
