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

// Package web defines web APIs to be used with harbourbridge frontend.
// Apart from schema conversion, this package involves API to update
// converted schema.
package session

import (
	"fmt"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
)

// UpdateSessionFile updates the content of session file with
// latest sessionState.Conv while also dumping schemas and report.
func UpdateSessionFile() error {
	sessionState := GetSessionState()

	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	_, err := conversion.WriteConvGeneratedFiles(sessionState.Conv, sessionState.DbName, sessionState.Driver, ioHelper.BytesRead, ioHelper.Out)
	if err != nil {
		return fmt.Errorf("Error encountered while updating session session file %w", err)
	}
	return nil
}
