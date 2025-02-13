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
	"os"
	"path/filepath"
	"sync"
)

var configFilePath string
var configFilePathMutex sync.Mutex

func getConfigFilePath() (string, error) {
	dirPath := "spanner_migration_tool_output/config"
	configFileName := "config.json"
	configFilePathMutex.Lock()         // Acquire the lock
	defer configFilePathMutex.Unlock() // Release the lock when done

	if configFilePath == "" {

		if _, err := os.Stat(dirPath); err != nil {
			err := os.MkdirAll(dirPath, os.ModePerm)
			if err != nil {
				log.Printf("Can't create directory %s: %v\n", dirPath, err)
				return "", err
			}
		}
		configFilePath = filepath.Join(dirPath, configFileName)
	}
	return configFilePath, nil
}
func GetSpannerConfig() (Config, error) {
	var c Config
	configFilePath, err := getConfigFilePath()
	if err != nil {
		log.Println(err)
		return c, err
	}

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

func SaveSpannerConfig(config Config) {
	configFilePath, err := getConfigFilePath()
	if err != nil {
		log.Println(err)
		return
	}
	f, err := os.OpenFile(configFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	file, err := json.MarshalIndent(config, "", " ")
	if err != nil {
		log.Println(err)
	}
	_, err = f.Write(file)

	if err != nil {
		log.Println(err)
	}
}

func TryInitializeSpannerConfig() Config {
	c, err := GetSpannerConfig()
	//Try load spanner config from environment variables and save to config
	if err != nil || c.GCPProjectID == "" || c.SpannerInstanceID == "" {
		projectId := os.Getenv("GCPProjectID")
		spProjectId := os.Getenv("SpannerProjectID")
		spInstanceId := os.Getenv("SpannerInstanceID")

		if spProjectId == "" {
			spProjectId = projectId
		}

		if projectId == "" || spInstanceId == "" {
			fmt.Println("Note: To store the sessions please set the environment variables 'GCPProjectID' and 'SpannerInstanceID'. You would set these as part of the migration workflow if you are using the Spanner migration tool Web UI.")
		} else {
			c.GCPProjectID = projectId
			c.SpannerProjectID = spProjectId
			c.SpannerInstanceID = spInstanceId
			SaveSpannerConfig(c)
		}
	}
	return c
}
