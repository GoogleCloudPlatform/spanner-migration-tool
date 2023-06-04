// Copyright 2022 Google LLC
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
package dms

import (
	"context"
	"fmt"
	"testing"
)

const (
	project          = "{project}"
	mysql_host       = "{host}"
	mysql_username   = "{username}"
	mysql_password   = "{password}"
	spanner_instance = "{instance}"
	spanner_database = "{database}"
)

func TestDMS(t *testing.T) {
	ctx := context.Background()

	err := createSpannerConn(ctx)
	if err != nil {
		t.Errorf("createSpannerConn(...) Error: %v", err)
	}
	err = createMySQLConn(ctx)
	if err != nil {
		t.Errorf("createMySQLConn(...) Error: %v", err)
	}
	commitID, err := createConvWorkspace(ctx)
	if err != nil {
		t.Errorf("createWorkspace(...) Error: %v", err)
	}
	err = createJob(ctx, commitID)
	if err != nil {
		t.Errorf("createJob(...) Error: %v", err)
	}
}

func createMySQLConn(ctx context.Context) error {
	source := SrcConnCfg{
		Project:             project,
		Location:            "us-central1",
		ConnectionProfileID: "mysql-conn",
		MySQLCfg: MySQLConnCfg{
			Host:     mysql_host,
			Port:     3306,
			Username: mysql_username,
			Password: mysql_password,
		},
	}
	return CreateMySQLConnectionProfile(ctx, source)
}

func createSpannerConn(ctx context.Context) error {
	dst := DstConnCfg{
		Project:             project,
		Location:            "us-central1",
		ConnectionProfileID: "span-conn",
		SpannerCfg: SpannerConnCfg{
			Project:  project,
			Instance: spanner_instance,
			Database: spanner_database,
		},
	}
	return CreateSpannerConnectionProfile(ctx, dst)
}

func createConvWorkspace(ctx context.Context) (string, error) {
	w := ConversionWorkspaceCfg{
		ID:       "conversion1",
		Project:  project,
		Location: "us-central1",
		SessionFile: SessionFileCfg{
			FileName:    "test_filename",
			FileContent: sessionFileContent,
		},
	}
	return CreateConversionWorkspace(ctx, w)
}

func createJob(ctx context.Context, commitID string) error {
	connFormat := "projects/%s/locations/%s/connectionProfiles/%s"
	sourceConn := fmt.Sprintf(connFormat, project, "us-central1", "mysql-conn")
	destConn := fmt.Sprintf(connFormat, project, "us-central1", "span-conn")
	workspaceID := fmt.Sprintf("projects/%s/locations/%s/conversionWorkspaces/%s", project, "us-central1", "conversion1")
	j := DMSJobCfg{
		ID:                          "jobID",
		Project:                     project,
		Location:                    "us-central1",
		SourceConnProfileID:         sourceConn,
		DestinationConnProfileID:    destConn,
		ConversionWorkspaceID:       workspaceID,
		ConversionWorkspaceCommitID: commitID,
	}
	return CreateDMSJob(ctx, j)
}

const sessionFileContent = `
{
	"SessionName": "NewSession",
	"EditorName": "",
	"DatabaseType": "mysql",
	"DatabaseName": "employees",
	"Notes": null,
	"Tags": null,
	"SpSchema": {
	  "t1": {
		"Name": "EMPLOYEES",
		"ColIds": [
		  "c1",
		  "c2",
		  "c3",
		  "c4",
		  "c5",
		  "c6"
		],
		"ColDefs": {
		  "c1": {
			"Name": "EmpNo",
			"T": {
			  "Name": "INT64",
			  "Len": 0,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: emp_no int",
			"Id": "c1"
		  },
		  "c2": {
			"Name": "BirthDate",
			"T": {
			  "Name": "DATE",
			  "Len": 0,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: birth_date date",
			"Id": "c2"
		  },
		  "c3": {
			"Name": "FirstName",
			"T": {
			  "Name": "STRING",
			  "Len": 14,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: first_name varchar(14)",
			"Id": "c3"
		  },
		  "c4": {
			"Name": "LastName",
			"T": {
			  "Name": "STRING",
			  "Len": 16,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: last_name varchar(16)",
			"Id": "c4"
		  },
		  "c5": {
			"Name": "Gender",
			"T": {
			  "Name": "STRING",
			  "Len": 10,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: gender string",
			"Id": "c5"
		  },
		  "c6": {
			"Name": "HireDate",
			"T": {
			  "Name": "DATE",
			  "Len": 0,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: hire_date date",
			"Id": "c6"
		  }
		},
		"PrimaryKeys": [
			{
				"ColId": "c1",
				"Desc": false,
				"Order": 1
			}
		],
		"ForeignKeys": null,
		"Indexes": null,
		"Id": "t1",
		"ParentId": "",
		"Comment": "Spanner schema for source table employees"
	  }
	},
	"SyntheticPKeys": null,
	"SrcSchema": {
	  "t1": {
		"Name": "employees",
		"Schema": "employees",
		"ColIds": [
		  "c1",
		  "c2",
		  "c3",
		  "c4",
		  "c5",
		  "c6"
		],
		"ColDefs": {
		  "c1": {
			"Name": "emp_no",
			"Type": {
			  "Name": "int",
			  "Mods": null,
			  "ArrayBounds": null
			},
			"NotNull": true,
			"Ignored": {
			  "Check": false,
			  "Identity": false,
			  "Default": false,
			  "Exclusion": false,
			  "ForeignKey": false,
			  "AutoIncrement": false
			},
			"Id": "c1"
		  },
		  "c2": {
			"Name": "birth_date",
			"Type": {
			  "Name": "date",
			  "Mods": null,
			  "ArrayBounds": null
			},
			"NotNull": true,
			"Ignored": {
			  "Check": false,
			  "Identity": false,
			  "Default": true,
			  "Exclusion": false,
			  "ForeignKey": false,
			  "AutoIncrement": false
			},
			"Id": "c2"
		  },
		  "c3": {
			"Name": "first_name",
			"Type": {
			  "Name": "varchar",
			  "Mods": [
				14
			  ],
			  "ArrayBounds": null
			},
			"NotNull": true,
			"Ignored": {
			  "Check": false,
			  "Identity": false,
			  "Default": true,
			  "Exclusion": false,
			  "ForeignKey": false,
			  "AutoIncrement": false
			},
			"Id": "c3"
		  },
		  "c4": {
			"Name": "last_name",
			"Type": {
			  "Name": "varchar",
			  "Mods": [
				16
			  ],
			  "ArrayBounds": null
			},
			"NotNull": true,
			"Ignored": {
			  "Check": false,
			  "Identity": false,
			  "Default": true,
			  "Exclusion": false,
			  "ForeignKey": false,
			  "AutoIncrement": false
			},
			"Id": "c4"
		  },
		  "c5": {
			"Name": "gender",
			"Type": {
			  "Name": "string",
			  "Mods": null,
			  "ArrayBounds": null
			},
			"NotNull": true,
			"Ignored": {
			  "Check": false,
			  "Identity": false,
			  "Default": true,
			  "Exclusion": false,
			  "ForeignKey": false,
			  "AutoIncrement": false
			},
			"Id": "c5"
		  },
		  "c6": {
			"Name": "hire_date",
			"Type": {
			  "Name": "date",
			  "Mods": null,
			  "ArrayBounds": null
			},
			"NotNull": true,
			"Ignored": {
			  "Check": false,
			  "Identity": false,
			  "Default": true,
			  "Exclusion": false,
			  "ForeignKey": false,
			  "AutoIncrement": false
			},
			"Id": "c6"
		  }
		},
		"PrimaryKeys": [
			{
				"ColId": "c1",
				"Desc": false,
				"Order": 1
			}
		],
		"ForeignKeys": null,
		"Indexes": null,
		"Id": "t1"
	  }
	},
	"Location": {},
	"TimezoneOffset": "+00:00",
	"TargetDb": "spanner",
	"UniquePKey": {}
  }
`
