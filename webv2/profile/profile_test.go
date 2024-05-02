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

package profile_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)


func TestVerifyJsonConfiguration(t *testing.T) {
	payload, err := json.Marshal(struct {
		configType string
		shardConfigurationDataflow  any
	}{})
	if err != nil {
		t.Fatal(err)
	}
	mvr:=conversion.MockValidateResources{}
	mvr.On("ValidateResourceGeneration", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	profileAPIHandler := profile.ProfileAPIHandler{
		ValidateResources: &mvr,
	}
	
	req, err := http.NewRequest("POST", "/VerifyJsonConfiguration", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json") 
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(profileAPIHandler.VerifyJsonConfiguration)
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}
