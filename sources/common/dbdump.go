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

package common

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

type BaseDbDump interface {
	GetBaseDdl() BaseToDdl
	ProcessDump(conv *internal.Conv, r *internal.Reader) error
}

// ProcessDbDump reads dump data from r and does schema or data conversion,
// depending on whether conv is configured for schema mode or data mode.
// In schema mode, this method incrementally builds a schema (updating conv).
// In data mode, this method uses this schema to convert data
// and writes it to Spanner, using the data sink specified in conv.
func ProcessDbDump(conv *internal.Conv, r *internal.Reader, baseDbDump BaseDbDump) error {
	err := baseDbDump.ProcessDump(conv, r)
	if err != nil {
		return err
	}
	if conv.SchemaMode() {
		SchemaToSpannerDDL(conv, baseDbDump.GetBaseDdl())
		conv.AddPrimaryKeys()
	}
	return nil
}
