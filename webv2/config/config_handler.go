// Copyright 2022 Google LLC

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

// Config represents Spanner Configuration for Spanner Session Management.
type Config struct {
	GCPProjectID      string `json:"GCPProjectID"`
	SpannerInstanceID string `json:"SpannerInstanceID"`
}

func GetConfig(w http.ResponseWriter, r *http.Request) {
	content, err := GetSpannerConfig()
	if err != nil {
		http.Error(w, "Data access error", http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(content)
}

func SetSpannerConfig(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}

	var c Config
	err = json.Unmarshal(reqBody, &c)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	err = saveSpannerConfigFile(c)
	if err != nil {
		log.Println(err)
		http.Error(w, "Data access error", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(c)
}
