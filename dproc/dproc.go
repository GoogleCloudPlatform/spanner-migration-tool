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
package dproc

import (
	"context"
	"fmt"
	"strings"

	dataproc "cloud.google.com/go/dataproc/apiv1"
	"cloud.google.com/go/dataproc/apiv1/dataprocpb"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"google.golang.org/api/option"
)

const (
	runtimeVersion string = "1.1"
	mainClass      string = "com.google.cloud.dataproc.templates.main.DataProcTemplate"
	fetchSize      string = "500"
	batchSize      string = "500"
	saveMode       string = "Append"
)

type DataprocRequestParams struct {
	Project       string
	TemplateArgs  []string
	JarFileUris   []string
	SubnetworkUri string
	Location      string
}

func GetDataprocRequestParams(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, srcSchema string, srcTable string, primaryKeys string, location string, subnet string) (DataprocRequestParams, error) {

	host := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.Host
	port := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.Port
	user := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.User
	pwd := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.Password
	spDb := targetProfile.Conn.Sp.Dbname
	spInstance := targetProfile.Conn.Sp.Instance
	spProject := targetProfile.Conn.Sp.Project
	if len(sourceProfile.Config.ShardConfigurationDataproc.DataprocConfig.Hostname) > 1 {
		host = sourceProfile.Config.ShardConfigurationDataproc.DataprocConfig.Hostname
	}
	if len(sourceProfile.Config.ShardConfigurationDataproc.DataprocConfig.Port) > 1 {
		port = sourceProfile.Config.ShardConfigurationDataproc.DataprocConfig.Port
	}

	jdbcParams := map[string]string{}
	if sourceProfile.Driver == constants.MYSQL {
		jdbcParams["url"] = fmt.Sprintf("jdbc:mysql://%s:%s/%s?user=%s&password=%s", host, port, srcSchema, user, pwd)
		jdbcParams["driver"] = "com.mysql.jdbc.Driver"
		jdbcParams["sql"] = fmt.Sprintf("select * from %s.%s", srcSchema, srcTable)
	} else {
		return DataprocRequestParams{}, fmt.Errorf("dataproc migration for source %s not supported", sourceProfile.Driver)
	}

	jarFileUris := []string{"file:///usr/lib/spark/external/spark-avro.jar",
		"gs://dataproc-templates-binaries/latest/java/dataproc-templates.jar",
		jdbcParams["jar"]}

	args := []string{"--template",
		"JDBCTOSPANNER",
		"--templateProperty",
		"project.id=" + spProject,
		"--templateProperty",
		"jdbctospanner.jdbc.url=" + jdbcParams["url"],
		"--templateProperty",
		"jdbctospanner.jdbc.driver.class.name=" + jdbcParams["driver"],
		"--templateProperty",
		"jdbctospanner.sql=" + jdbcParams["sql"],
		"--templateProperty",
		"jdbctospanner.output.instance=" + spInstance,
		"--templateProperty",
		"jdbctospanner.output.database=" + spDb,
		"--templateProperty",
		"jdbctospanner.output.table=" + srcTable,
		"--templateProperty",
		"jdbctospanner.output.primaryKey=" + primaryKeys,
		"--templateProperty",
		"jdbctospanner.output.saveMode=" + saveMode,
		"--templateProperty",
		"jdbctospanner.output.batch.size=" + batchSize,
		"--templateProperty",
		"jdbctospanner.jdbc.fetchsize=" + fetchSize}

	dataprocRequestParams := DataprocRequestParams{
		Project:       spProject,
		TemplateArgs:  args,
		JarFileUris:   jarFileUris,
		SubnetworkUri: subnet,
		Location:      location,
	}

	return dataprocRequestParams, nil
}

func CreateDataprocBatchClient(location string) (*dataproc.BatchControllerClient, error) {
	ctx := context.Background()
	batchEndpoint := fmt.Sprintf("%s-dataproc.googleapis.com:443", location)
	batchClient, err := dataproc.NewBatchControllerClient(ctx, option.WithEndpoint(batchEndpoint))
	return batchClient, err
}

func TriggerDataprocTemplate(batchClient *dataproc.BatchControllerClient, srcTable string, srcSchema string, primaryKeys string, dataprocRequestParams DataprocRequestParams) (string, error) {
	ctx := context.Background()

	fmt.Printf("Triggering Dataproc template for %s.%s\n", srcSchema, srcTable)
	req := &dataprocpb.CreateBatchRequest{
		Parent: "projects/" + dataprocRequestParams.Project + "/locations/" + dataprocRequestParams.Location,
		Batch: &dataprocpb.Batch{
			RuntimeConfig: &dataprocpb.RuntimeConfig{
				Version: runtimeVersion,
			},
			EnvironmentConfig: &dataprocpb.EnvironmentConfig{
				ExecutionConfig: &dataprocpb.ExecutionConfig{
					Network: &dataprocpb.ExecutionConfig_SubnetworkUri{
						SubnetworkUri: dataprocRequestParams.SubnetworkUri,
					},
				},
			},
			BatchConfig: &dataprocpb.Batch_SparkBatch{
				SparkBatch: &dataprocpb.SparkBatch{
					Driver: &dataprocpb.SparkBatch_MainClass{
						MainClass: mainClass,
					},
					Args:        dataprocRequestParams.TemplateArgs,
					JarFileUris: dataprocRequestParams.JarFileUris,
				},
			},
		},
	}

	op, err := batchClient.CreateBatch(ctx, req)
	if err != nil {
		fmt.Printf("error creating the batch: %s\n", err.Error())
		return "", err
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		fmt.Printf("error completing the batch: %s\n", err.Error())
		fmt.Printf("Failing data migration from Dataproc template for %s.%s with batch id: %s\n", srcSchema, srcTable, resp.GetName())
		return resp.GetName(), err
	}

	batchName := resp.GetName()

	splittedBatchName := strings.Split(batchName, "/")
	jobId := splittedBatchName[5]

	return jobId, err
}
