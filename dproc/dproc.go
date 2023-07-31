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
	"github.com/cloudspannerecosystem/harbourbridge/internal"
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

func getJdbcSql(conv *internal.Conv, srcDriver string, spTableID string, primaryKeys string) string {
	srcTable := conv.SrcSchema[spTableID].Name
	srcSchema := conv.SrcSchema[spTableID].Schema
	const tableAlias string = "t"

	var selectCols []string
	for _, colId := range conv.SpSchema[spTableID].ColIds {
		if conv.SrcSchema[spTableID].ColDefs[colId].Name != "" {
			selectCols = append(selectCols, fmt.Sprintf("%s.%s", tableAlias, conv.SrcSchema[spTableID].ColDefs[colId].Name))
		}
	}
	if srcDriver == constants.MYSQL {
		if primaryKeys == "" {
			return fmt.Sprintf("select uuid() as synth_id, %s from %s.%s as %s", strings.Join(selectCols, ", "), srcSchema, srcTable, tableAlias)
		}
	}
	return fmt.Sprintf("select %s from %s.%s as %s", strings.Join(selectCols, ", "), srcSchema, srcTable, tableAlias)
}

func GetDataprocRequestParams(conv *internal.Conv, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, spTableID string, primaryKeys string, location string, subnet string) (DataprocRequestParams, error) {
	host := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.Host
	port := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.Port
	user := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.User
	pwd := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.Password
	srcDb := sourceProfile.Config.ShardConfigurationDataproc.SchemaSource.DbName
	spDb := targetProfile.Conn.Sp.Dbname
	spInstance := targetProfile.Conn.Sp.Instance
	spProject := targetProfile.Conn.Sp.Project
	if len(sourceProfile.Config.ShardConfigurationDataproc.DataprocConfig.Hostname) > 1 {
		host = sourceProfile.Config.ShardConfigurationDataproc.DataprocConfig.Hostname
	}
	if len(sourceProfile.Config.ShardConfigurationDataproc.DataprocConfig.Port) > 1 {
		port = sourceProfile.Config.ShardConfigurationDataproc.DataprocConfig.Port
	}
	spTableName := conv.SpSchema[spTableID].Name

	jdbcParams := map[string]string{}
	// TODO: replace hardcoded `synth_id` with a variable/constant
	if sourceProfile.Driver == constants.MYSQL {
		jdbcParams["url"] = fmt.Sprintf("jdbc:mysql://%s:%s/%s?user=%s&password=%s", host, port, srcDb, user, pwd)
		jdbcParams["driver"] = "com.mysql.jdbc.Driver"
		jdbcParams["jar"] = "gs://dataproc-templates/jars/mysql-connector-java.jar"
	} else {
		return DataprocRequestParams{}, fmt.Errorf("dataproc migration for source %s not supported", sourceProfile.Driver)
	}
	jdbcParams["sql"] = getJdbcSql(conv, sourceProfile.Driver, spTableID, primaryKeys)

	outputPrimaryKeys := primaryKeys
	if primaryKeys == "" {
		outputPrimaryKeys = "synth_id"
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
		"jdbctospanner.output.table=" + spTableName,
		"--templateProperty",
		"jdbctospanner.output.primaryKey=" + outputPrimaryKeys,
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

func CreateDataprocBatchClient(ctx context.Context, location string) (*dataproc.BatchControllerClient, error) {
	batchEndpoint := fmt.Sprintf("%s-dataproc.googleapis.com:443", location)
	batchClient, err := dataproc.NewBatchControllerClient(ctx, option.WithEndpoint(batchEndpoint))
	return batchClient, err
}

func TriggerDataprocTemplate(ctx context.Context, batchClient *dataproc.BatchControllerClient, dataprocRequestParams DataprocRequestParams) (*dataproc.CreateBatchOperation, error) {
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
	return op, err
}
