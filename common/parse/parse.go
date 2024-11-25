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

// Package utils contains common helper functions used across multiple other packages.
// Utils should not import any Spanner migration tool packages.
package parse

import (
	"fmt"
	"strings"
)

// parseURI parses an unknown URI string that could be a database, instance or project URI.
func parseURI(URI string) (project, instance, dbName string) {
	project, instance, dbName = "", "", ""
	if strings.Contains(URI, "databases") {
		project, instance, dbName = ParseDbURI(URI)
	} else if strings.Contains(URI, "instances") {
		project, instance = parseInstanceURI(URI)
	} else if strings.Contains(URI, "projects") {
		project = parseProjectURI(URI)
	}
	return
}

func ParseDbURI(dbURI string) (project, instance, dbName string) {
	split := strings.Split(dbURI, "/databases/")
	project, instance = parseInstanceURI(split[0])
	dbName = split[1]
	return
}

func parseInstanceURI(instanceURI string) (project, instance string) {
	split := strings.Split(instanceURI, "/instances/")
	project = parseProjectURI(split[0])
	instance = split[1]
	return
}

func parseProjectURI(projectURI string) (project string) {
	split := strings.Split(projectURI, "/")
	project = split[1]
	return
}

// AnalyzeError inspects an error returned from Cloud Spanner and adds information
// about potential root causes e.g. authentication issues.
func AnalyzeError(err error, URI string) error {
	project, instance, _ := parseURI(URI)
	e := strings.ToLower(err.Error())
	if ContainsAny(e, []string{"unauthenticated", "cannot fetch token", "default credentials"}) {
		return fmt.Errorf("%w."+`
Possible cause: credentials are mis-configured. Do you need to run

  gcloud auth application-default login

or configure environment variable GOOGLE_APPLICATION_CREDENTIALS.
See https://cloud.google.com/docs/authentication/getting-started`, err)
	}
	if ContainsAny(e, []string{"instance not found"}) && instance != "" {
		return fmt.Errorf("%w.\n"+`
Possible cause: Spanner instance specified via instance option does not exist.
Please check that '%s' is correct and that it is a valid Spanner
instance for project %s`, err, instance, project)
	}
	return err
}

func ContainsAny(s string, l []string) bool {
	for _, a := range l {
		if strings.Contains(s, a) {
			return true
		}
	}
	return false
}
