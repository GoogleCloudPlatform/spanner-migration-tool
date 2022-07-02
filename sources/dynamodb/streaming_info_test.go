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
package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInfo_Unexpected(t *testing.T) {
	streamInfo := MakeInfo()
	streamInfo.Unexpected("testing-unexpecteds-faced-1")
	streamInfo.Unexpected("testing-unexpecteds-faced-2")
	assert.Equal(t, int64(2), streamInfo.TotalUnexpecteds())
}

func TestInfo_TotalRecords(t *testing.T) {
	streamInfo := MakeInfo()
	testTables := [2]string{"testTable1", "testTable2"}
	for i := 0; i < 2; i++ {
		streamInfo.Records[testTables[i]] = make(map[string]int64)
	}
	streamInfo.Records[testTables[0]]["INSERT"] = 37
	streamInfo.Records[testTables[1]]["MODIFY"] = 3
	assert.Equal(t, int64(40), streamInfo.TotalRecords())
}
