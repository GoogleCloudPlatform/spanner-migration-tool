package dproc

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/stretchr/testify/assert"
)

// TODO: add unit tests for otjer methods in dproc package

func TestGetDataprocRequestParams(t *testing.T) {
	type paramsStruct struct {
		sourceProfile profiles.SourceProfile
		targetProfile profiles.TargetProfile
		srcSchema     string
		srcTable      string
		primaryKeys   string
		location      string
		subnet        string
	}

	sourceProfile, _ := profiles.NewSourceProfile(fmt.Sprintf("config=%v", filepath.Join("..", "test_data", "mysql_shard_dataproc.cfg")), "mysql")
	sourceProfile.Driver, _ = sourceProfile.ToLegacyDriver("mysql")
	targetProfile, _ := profiles.NewTargetProfile("project=test-project,instance=sp-test-instance,dbName=sp-test-db")

	// TODO: add more test cases
	testCases := []struct {
		name          string
		params        paramsStruct
		want          DataprocRequestParams
		errorExpected bool
	}{
		{
			name: "empty/invalid source profile and target profile",
			params: paramsStruct{
				sourceProfile: profiles.SourceProfile{},
				targetProfile: profiles.TargetProfile{},
				srcSchema:     "test_schema",
				srcTable:      "test_table",
				primaryKeys:   "pk1,pk2",
				location:      "us-central1",
				subnet:        "projects/test-project/regions/us-central1/subnetworks/test-subnet",
			},
			want:          DataprocRequestParams{},
			errorExpected: true,
		},
		{
			name: "valid mysql source profile and target profile",
			params: paramsStruct{
				sourceProfile: sourceProfile,
				targetProfile: targetProfile,
				srcSchema:     "test_schema",
				srcTable:      "test_table",
				primaryKeys:   "pk1,pk2",
				location:      "us-central1",
				subnet:        "projects/test-project/regions/us-central1/subnetworks/test-subnet",
			},
			want: DataprocRequestParams{
				Project: "test-project",
				TemplateArgs: []string{"--template",
					"JDBCTOSPANNER",
					"--templateProperty",
					"project.id=test-project",
					"--templateProperty",
					"jdbctospanner.jdbc.url=jdbc:mysql://0.0.0.0:3306/test_schema?user=test&password=test",
					"--templateProperty",
					"jdbctospanner.jdbc.driver.class.name=com.mysql.jdbc.Driver",
					"--templateProperty",
					"jdbctospanner.sql=select * from test_schema.test_table",
					"--templateProperty",
					"jdbctospanner.output.instance=sp-test-instance",
					"--templateProperty",
					"jdbctospanner.output.database=sp-test-db",
					"--templateProperty",
					"jdbctospanner.output.table=test_table",
					"--templateProperty",
					"jdbctospanner.output.primaryKey=pk1,pk2",
					"--templateProperty",
					"jdbctospanner.output.saveMode=Append",
					"--templateProperty",
					"jdbctospanner.output.batch.size=500",
					"--templateProperty",
					"jdbctospanner.jdbc.fetchsize=500"},
				JarFileUris: []string{"file:///usr/lib/spark/external/spark-avro.jar",
					"gs://dataproc-templates-binaries/latest/java/dataproc-templates.jar",
					"gs://dataproc-templates/jars/mysql-connector-java.jar"},
				SubnetworkUri: "projects/test-project/regions/us-central1/subnetworks/test-subnet",
				Location:      "us-central1",
			},
			errorExpected: false,
		},
	}

	for _, tc := range testCases {
		got, err := GetDataprocRequestParams(tc.params.sourceProfile, tc.params.targetProfile, tc.params.srcSchema, tc.params.srcTable, tc.params.primaryKeys, tc.params.location, tc.params.subnet)
		assert.Equal(t, got, tc.want, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil)
	}
}
