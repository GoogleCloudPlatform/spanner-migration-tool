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

/*
Package utils contains common helper functions used across multiple other packages.
Utils should not import any Spanner migration tool packages.
*/
package utils

import (
	"fmt"
	"net/url"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
)

func ParseGCSFilePath(filePath string) (*url.URL, error) {
	if len(filePath) == 0 {
		return nil, fmt.Errorf("found empty GCS path")
	}
	if filePath[len(filePath)-1] != '/' {
		filePath = filePath + "/"
	}
	u, err := url.Parse(filePath)
	if err != nil {
		return nil, fmt.Errorf("parseFilePath: unable to parse file path %s", filePath)
	}
	if u.Scheme != constants.GCS_SCHEME {
		return nil, fmt.Errorf("not a valid GCS path: %s, should start with 'gs'", filePath)
	}
	return u, nil
}
