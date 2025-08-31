// Copyright 2024 Google LLC
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

// OverridesFile represents the overrides file format containing schema mapping information
type OverridesFile struct {
	RenamedTables  map[string]string            `json:"renamedTables"`
	RenamedColumns map[string]map[string]string `json:"renamedColumns"`
}

// ExtractOverridesFromConv extracts renamed tables and columns from the conv object
func ExtractOverridesFromConv(conv *Conv) *OverridesFile {
	overrides := &OverridesFile{
		RenamedTables:  make(map[string]string),
		RenamedColumns: make(map[string]map[string]string),
	}

	// Extract renamed tables and columns from ToSpanner mapping
	for srcTableName, nameAndCols := range conv.ToSpanner {
		if nameAndCols.Name != srcTableName {
			overrides.RenamedTables[srcTableName] = nameAndCols.Name
		}

		// Extract renamed columns
		if len(nameAndCols.Cols) > 0 {
			for srcColName, spColName := range nameAndCols.Cols {
				if spColName != srcColName {
					if _, ok := overrides.RenamedColumns[srcTableName]; !ok {
						overrides.RenamedColumns[srcTableName] = make(map[string]string)
					}
					overrides.RenamedColumns[srcTableName][srcColName] = spColName
				}
			}
		}
	}

	return overrides
}
