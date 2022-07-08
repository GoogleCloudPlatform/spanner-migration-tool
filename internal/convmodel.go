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

package internal

import (
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type ConvViewModel struct {
	Conv         *Conv
	SpSchemaMap  ddl.Schema
	SrcSchemaMap map[string]schema.Table
}

func MakeConvViewModel() ConvViewModel {

	c := ConvViewModel{
		Conv:         MakeConv(),
		SpSchemaMap:  ddl.NewSchema(),
		SrcSchemaMap: make(map[string]schema.Table),
	}

	return c
}
