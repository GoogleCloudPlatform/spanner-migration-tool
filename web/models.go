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

package web

import (
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

type DriverConfig struct {
	Driver   string `json:"Driver"`
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Database string `json:"Database"`
	User     string `json:"User"`
	Password string `json:"Password"`
}

type DumpConfig struct {
	Driver   string `json:"Driver"`
	FilePath string `json:"Path"`
}

type Session struct {
	Driver    string    `json:"driver"`
	FilePath  string    `json:"path"`
	FileName  string    `json:"fileName"`
	CreatedAt time.Time `json:"createdAt"`
}

type Summary struct {
	Heading string
	Lines   []string
	Rate    string
}
type typeIssue struct {
	T     string
	Issue internal.SchemaIssue
	Brief string
}

type updateCol struct {
	Removed bool   `json:"Removed"`
	Rename  string `json:"Rename"`
	PK      string `json:"PK"`
	NotNull string `json:"NotNull"`
	ToType  string `json:"ToType"`
}
type updateTable struct {
	UpdateCols map[string]updateCol `json:"UpdateCols"`
}
