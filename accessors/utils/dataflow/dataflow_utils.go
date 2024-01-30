// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This is a package is kept with accessors because some functions import other accessors.
// The common/utils package should not import any SMT dependency.
package dataflowutils

import (
	"context"
	"encoding/json"

	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
	dataflowaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	storageaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
)

func UnmarshalDataflowTuningConfig(ctx context.Context, sc storageclient.StorageClient, sa storageaccessor.StorageAccessor, filePath string) (dataflowaccessor.DataflowTuningConfig, error) {
	jsonStr, err := sa.ReadAnyFile(ctx, sc, filePath)
	if err != nil {
		return dataflowaccessor.DataflowTuningConfig{}, err
	}
	tuningCfg := dataflowaccessor.DataflowTuningConfig{}
	err = json.Unmarshal([]byte(jsonStr), &tuningCfg)
	if err != nil {
		return dataflowaccessor.DataflowTuningConfig{}, err
	}
	return tuningCfg, nil
}
