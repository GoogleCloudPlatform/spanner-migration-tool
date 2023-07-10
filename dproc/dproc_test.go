package dproc

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/stretchr/testify/assert"
)

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

	sourceProfileMySQL, _ := profiles.NewSourceProfile(fmt.Sprintf("config=%v", filepath.Join("..", "test_data", "mysql_shard_dataproc.cfg")), "mysql")
	sourceProfileMySQL.Driver, _ = sourceProfileMySQL.ToLegacyDriver("mysql")

	sourceProfilePostgres, _ := profiles.NewSourceProfile(fmt.Sprintf("config=%v", filepath.Join("..", "test_data", "postgres_shard_dataproc.cfg")), "postgres")
	sourceProfilePostgres.Driver, _ = sourceProfilePostgres.ToLegacyDriver("postgres")
	fmt.Println(sourceProfilePostgres.Driver)

	sourceProfileOracle, _ := profiles.NewSourceProfile(fmt.Sprintf("config=%v", filepath.Join("..", "test_data", "mysql_shard_dataproc.cfg")), "oracle")
	sourceProfileOracle.Driver, _ = sourceProfileOracle.ToLegacyDriver("oracle")

	targetProfile, _ := profiles.NewTargetProfile("project=test-project,instance=sp-test-instance,dbName=sp-test-db")

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
			name: "valid oracle source profile and target profile",
			params: paramsStruct{
				sourceProfile: sourceProfileOracle,
				targetProfile: targetProfile,
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
				sourceProfile: sourceProfileMySQL,
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
					"jdbctospanner.jdbc.url=jdbc:mysql://0.0.0.0:3306/test?user=test&password=test",
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
		{
			name: "valid postgres source profile and target profile",
			params: paramsStruct{
				sourceProfile: sourceProfilePostgres,
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
					"jdbctospanner.jdbc.url=jdbc:postgresql://0.0.0.0:5432/test?user=test&password=test",
					"--templateProperty",
					"jdbctospanner.jdbc.driver.class.name=org.postgresql.Driver",
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
					"gs://dataproc-templates/jars/postgresql-42.2.6.jar"},
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
