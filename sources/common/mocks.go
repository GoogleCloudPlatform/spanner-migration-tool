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

package common

import (
	"sync"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/stretchr/testify/mock"
)

type MockInfoSchema struct {
    mock.Mock
}

func (mis *MockInfoSchema) GenerateSrcSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int) (int, error) {
	args := mis.Called(conv, infoSchema, numWorkers)
	return args.Get(0).(int), args.Error(1)
}
func (mis *MockInfoSchema) ProcessData(conv *internal.Conv, infoSchema InfoSchema, additionalAttributes internal.AdditionalDataAttributes) {}
func (mis *MockInfoSchema) SetRowStats(conv *internal.Conv, infoSchema InfoSchema) {}
func (mis *MockInfoSchema) processTable(conv *internal.Conv, table SchemaAndName, infoSchema InfoSchema) (schema.Table, error)  {
	args := mis.Called(conv, table, infoSchema)
	return args.Get(0).(schema.Table), args.Error(1)
}
func (mis *MockInfoSchema) GetIncludedSrcTablesFromConv(conv *internal.Conv) (schemaToTablesMap map[string]internal.SchemaDetails, err error)   {
	args := mis.Called(conv)
	return args.Get(0).(map[string]internal.SchemaDetails), args.Error(1)
}

type MockUtilsOrder struct {
    mock.Mock
}

func (muo *MockUtilsOrder) initPrimaryKeyOrder(conv *internal.Conv) {}

func (muo *MockUtilsOrder) initIndexOrder(conv *internal.Conv) {}

type MockSchemaToSpanner struct {
    mock.Mock
}

func (mss *MockSchemaToSpanner) SchemaToSpannerDDL(conv *internal.Conv, toddl ToDdl) error {
	args := mss.Called(conv, toddl)
	return args.Error(0)
}

func (mss *MockSchemaToSpanner) SchemaToSpannerDDLHelper(conv *internal.Conv, toddl ToDdl, srcTable schema.Table, isRestore bool) error {
	args := mss.Called(conv, toddl, srcTable, isRestore)
	return args.Error(0)
}

type MockProcessSchema struct {
    mock.Mock
}

func (mps *MockProcessSchema) ProcessSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int, attributes internal.AdditionalSchemaAttributes, s SchemaToSpannerInterface, uo UtilsOrderInterface, is InfoSchemaInterface) error {
	args := mps.Called(conv, infoSchema, numWorkers, attributes, s, uo, is)
	return args.Error(0)
}

type MockRunParallelTasks[I any, O any] struct {
	mock.Mock
}

func (mrpt *MockRunParallelTasks[I, O]) RunParallelTasks(input []I, numWorkers int, f func(i I, mutex *sync.Mutex) TaskResult[O],
fastExit bool) ([]TaskResult[O], error) {
	args := mrpt.Called(input, numWorkers, f, fastExit)
	return args.Get(0).([]TaskResult[O]), args.Error(1)
}