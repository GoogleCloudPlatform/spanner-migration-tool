// Copyright 2023 Google LLC
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
package streaming

import (
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

// This file contains the common structs for mapping to Harbourbridge's underlying metadata tables

// Table structs - These structs map to the Spanner metadata tables

// Stores the migration job level data post orchestration of a migration job
type SmtJobs struct {
	JobId               string
	JobName             string
	JobType             string
	JobData             string
	Dialect             string
	SpannerDatabaseName string
	CreatedAt           time.Time
}

// Stores the resource level execution data post orchestration of a migration job
type SmtResources struct {
	ResourceId   string
	JobId        string
	ExternalId   string
	ResourceName string
	ResourceType string
	ResourceData string
	CreatedAt    time.Time
}

// Storage structs - these structs map to JSON data stored inside the metadata

type MinimaldowntimeJobData struct {
	IsSharded bool
	Session *internal.Conv
}

type MinimalDowntimeResourceData struct {
	DataShardId string
	ResourcePayload string
}