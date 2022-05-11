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
	"io/ioutil"
	"log"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/shared"
)

var configFilePath string = "./webv2/config.json"

func GetSpannerConfig() (Config, error) {
	var c Config
	content, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Println(err)
		return c, err
	}

	err = json.Unmarshal(content, &c)
	if err != nil {
		log.Println(err)
		return c, err
	}
	return c, nil
}

// Gets configuration from environment variables and saves it in config file.
func LoadConfigFromEnv() {
	var c Config
	c.GCPProjectID = os.Getenv("GCPProjectID")
	c.SpannerInstanceID = os.Getenv("SpannerInstanceID")

	if c.GCPProjectID == "" || c.SpannerInstanceID == "" {
		log.Println("warning : please set the environment variables - GCPProjectID and SpannerInstanceID")
	}

	f, err := os.OpenFile(configFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	file, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		log.Println(err)
	}
	_, err = f.Write(file)

	if err != nil {
		log.Println(err)
	}
}

// Saves spanner configuration in configuration file.
func saveSpannerConfigFile(c Config) error {
	f, err := os.OpenFile(configFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Println(err)
		return err
	}
	defer f.Close()

	file, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = f.Write(file)
	if err != nil {
		log.Println(err)
		return err
	}

	sessionState := session.GetSessionState()
	isValid := shared.PingMetadataDb(c.GCPProjectID, c.SpannerInstanceID)
	if !isValid {
		sessionState.IsOffline = true
		sessionState.GCPProjectID = ""
		sessionState.SpannerInstanceID = ""
	} else {
		sessionState.GCPProjectID = c.GCPProjectID
		sessionState.SpannerInstanceID = c.SpannerInstanceID
		sessionState.IsOffline = false
	}

	return nil
}
