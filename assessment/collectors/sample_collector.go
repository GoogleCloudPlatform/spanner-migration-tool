/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package assessment

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
)

// This is sample implementation to bootstrap.
// Once the collectors are implemented, this can be deleted
type SampleCollector struct {
	tables []utils.TableAssessmentInfo
}

func CreateSampleCollector() (SampleCollector, error) {
	logger.Log.Info("initializing sample collector")
	tb := []utils.TableAssessmentInfo{
		{Name: "t1"},
		{Name: "t2"},
	}
	return SampleCollector{
		tables: tb,
	}, nil
}

func (c SampleCollector) ListTables() []string {
	var tableArray []string = []string{}

	for i := range c.tables {
		tableArray = append(tableArray, c.tables[i].Name)
	}
	return tableArray
}
