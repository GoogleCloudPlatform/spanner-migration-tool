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

	err := CreateSpannerConn(ctx)
	if err != nil {
		t.Errorf(" createSpannerConn(...) Error: %v ", err)
	}
	err = CreateMySQLConn(ctx, "mysql-conn1")
	if err != nil {
		t.Errorf(" createMySQLConn(...) Error: %v ", err)
	}
	commitID, err := CreateConvWorkspace(ctx, "mysql-conn1")
	if err != nil {
		t.Errorf(" createWorkspace(...) Error: %v ", err)
	}
	// Shard1
	t.Logf(" commitId=%v, err=%v ", commitID, err)
	err = CreateJob(ctx, commitID, "mysql-conn1", "job1")
	if err != nil {
		t.Errorf(" createJob(...) Error: %v ", err)
	}

	// Shard2
	err = CreateMySQLConn(ctx, "mysql-conn2")
	if err != nil {
		t.Errorf(" createMySQLConn(...) Error: %v ", err)
	}
	err = CreateJob(ctx, commitID, "mysql-conn2", "job2")
	if err != nil {
		t.Errorf(" createJob(...) Error: %v ", err)
	}
	ips, err := fetchStaticIps(ctx, project, "us-central1")
	t.Logf("ips:%v, err:%v", ips, err)
}

func CreateMySQLConn(ctx context.Context, id string) error {
	if id == "" {
		id = "mysql-conn"
	}
	source := SrcConnCfg{
		ConnectionProfileID: ResourceIdentifier{Project: project, Location: "us-central1", ID: id},
		MySQLCfg: MySQLConnCfg{
			Host:     mysql_host,
			Port:     3306,
			Username: mysql_username,
			Password: mysql_password,
		},
	}
	err := TestMySQLConnectionProfile(ctx, source)
	if err != nil {
		return err
	}
	return CreateMySQLConnectionProfile(ctx, source)
}

func CreateSpannerConn(ctx context.Context) error {
	dst := DstConnCfg{
		ConnectionProfileID: ResourceIdentifier{Project: project, Location: "us-central1", ID: "span-conn1"},
		SpannerCfg: SpannerConnCfg{
			Project:  project,
			Instance: spanner_instance,
			Database: spanner_database,
		},
	}
	err := TestSpannerConnectionProfile(ctx, dst)
	if err != nil {
		return err
	}
	return CreateSpannerConnectionProfile(ctx, dst)
}

func CreateConvWorkspace(ctx context.Context, mySQLConnID string) (string, error) {
	w := ConversionWorkspaceCfg{
		ConversionWorkspaceID: ResourceIdentifier{Project: project, Location: "us-central1", ID: "conversion1"},
		SessionFile: SessionFileCfg{
			FileName:    "test_filename",
			FileContent: SessionFileContent,
		},
		SourceConnectionProfileID:      ResourceIdentifier{Project: project, Location: "us-central1", ID: mySQLConnID},
		DestinationConnectionProfileID: ResourceIdentifier{Project: project, Location: "us-central1", ID: "span-conn"},
	}
	return createConversionWorkspace(ctx, w)
}

func CreateJob(ctx context.Context, commitID string, mysqlConnID string, jobID string) error {
	j := DMSJobCfg{
		JobID:                       ResourceIdentifier{Project: project, Location: "us-central1", ID: jobID},
		SourceConnProfileID:         ResourceIdentifier{Project: project, Location: "us-central1", ID: mysqlConnID},
		DestinationConnProfileID:    ResourceIdentifier{Project: project, Location: "us-central1", ID: "span-conn"},
		ConversionWorkspaceID:       ResourceIdentifier{Project: project, Location: "us-central1", ID: "conversion1"},
		ConversionWorkspaceCommitID: commitID,
	}
	err := createDMSJob(ctx, j)
	if err != nil {
		return err
	}
	return launchDMSJob(ctx, j)
}

const SessionFileContent = `
{
	"SessionName": "NewSession",
	"EditorName": "",
	"DatabaseType": "mysql",
	"DatabaseName": "single",
	"Dialect": "google_standard_sql",
	"Notes": null,
	"Tags": null,
	"SpSchema": {
	  "t187": {
		"Name": "ORDERS",
		"ColIds": [
		  "c188",
		  "c189",
		  "c190",
		  "c191",
		  "c192"
		],
		"ColDefs": {
		  "c188": {
			"Name": "OrderID",
			"T": {
			  "Name": "INT64",
			  "Len": 0,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: OrderID int",
			"Id": "c188"
		  },
		  "c189": {
			"Name": "CustomerID",
			"T": {
			  "Name": "INT64",
			  "Len": 0,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: CustomerID int",
			"Id": "c189"
		  },
		  "c190": {
			"Name": "Sts",
			"T": {
			  "Name": "STRING",
			  "Len": 2000,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: Status varchar(2000)",
			"Id": "c190"
		  },
		  "c191": {
			"Name": "SID",
			"T": {
			  "Name": "INT64",
			  "Len": 0,
			  "IsArray": false
			},
			"NotNull": false,
			"Comment": "From: SalesmanID int",
			"Id": "c191"
		  },
		  "c192": {
			"Name": "OrderDate",
			"T": {
			  "Name": "DATE",
			  "Len": 0,
			  "IsArray": false
			},
			"NotNull": true,
			"Comment": "From: OrderDate date",
			"Id": "c192"
		  }
		},
		"PrimaryKeys": [
		  {
			"ColId": "c188",
			"Desc": false,
			"Order": 1
		  }
		],
		"ForeignKeys": null,
		"Indexes": null,
		"ParentId": "",
		"Comment": "Spanner schema for source table ORDERS",
		"Id": "t187"
	  }
	},
	"SyntheticPKeys": {},
	"SrcSchema": {
	  "t187": {
		"Name": "ORDERS",
		"Schema": "single",
		"ColIds": [
		  "c188",
		  "c189",
		  "c190",
		  "c191",
		  "c192"
		],
		"ColDefs": {
		  "c188": {
			"Name": "OrderID",
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
			"Id": "c188"
		  },
		  "c189": {
			"Name": "CustomerID",
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
			"Id": "c189"
		  },
		  "c190": {
			"Name": "Status",
			"Type": {
			  "Name": "varchar",
			  "Mods": [
				2000
			  ],
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
			"Id": "c190"
		  },
		  "c191": {
			"Name": "SalesmanID",
			"Type": {
			  "Name": "int",
			  "Mods": null,
			  "ArrayBounds": null
			},
			"NotNull": false,
			"Ignored": {
			  "Check": false,
			  "Identity": false,
			  "Default": false,
			  "Exclusion": false,
			  "ForeignKey": false,
			  "AutoIncrement": false
			},
			"Id": "c191"
		  },
		  "c192": {
			"Name": "OrderDate",
			"Type": {
			  "Name": "date",
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
			"Id": "c192"
		  }
		},
		"PrimaryKeys": [
		  {
			"ColId": "c188",
			"Desc": false,
			"Order": 1
		  }
		],
		"ForeignKeys": null,
		"Indexes": null,
		"Id": "t187"
	  }
	},
	"Location": {},
	"TimezoneOffset": "+00:00",
	"UniquePKey": {},
	"SpDialect": "google_standard_sql",
	"Rules": []
  }`
