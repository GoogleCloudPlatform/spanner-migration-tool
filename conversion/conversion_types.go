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

package conversion

import "context"

type ConnectionProfile struct {
	// Project Id of the resource
	ProjectId string
	// Datashard Id for the resource
	DatashardId string
	// Name of connection profile
	Id string
	// If true, don't create resource, only validate if creation is possible. If false, create resource.
	ValidateOnly bool
	// If true, create source connection profile, else create target connection profile and gcs bucket.
	IsSource bool
	// For source connection profile host of MySql instance
	Host string
	// For source connection profile port of MySql instance
	Port string
	// For source connection profile password of MySql instance
	Password string
	// For source connection profile user name of MySql instance
	User string
	// Region of connection profile to be created
	Region string
	// For target connection profile name of gcs bucket to be created
	BucketName string
}

type ConnectionProfileReq struct {
	ConnectionProfile ConnectionProfile
	Error             error
	Ctx               context.Context
}