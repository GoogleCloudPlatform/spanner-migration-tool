package expressionsapi_test

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

var (
	projectID  string
	instanceID string

	ctx           context.Context
)

func TestMain(m *testing.M) {
	cleanup := initIntegrationTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func initIntegrationTests() (cleanup func()) {
	projectID = os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}

	if projectID == "" {
		log.Println("Integration tests skipped: SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if instanceID == "" {
		log.Println("Integration tests skipped: SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID is missing")
		return noop
	}
	return func() {}
}

func prepareIntegrationTest(t *testing.T) string {
	tmpdir, err := ioutil.TempDir(".", "int-test-")
	if err != nil {
		log.Fatal(err)
	}
	return tmpdir
}

func TestIntegration_VerifyExpressions(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)
	ev, err := expressions_api.NewExpressionVerificationAccessorImpl(ctx, projectID, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	conv := internal.MakeConv()
	ReadSessionFile(conv, "../../test_data/session_expression_verify.json")
	input := internal.VerifyExpressionsInput{
		Conv:     conv,
		Source:   "mysql",
		ExpressionDetailList: []internal.ExpressionDetail{
			{
				Expression: "id > 10",
				Type:       "CHECK", ReferenceElement: internal.ReferenceElement{Name: "Books"}, ExpressionId: "1"}},
	}
	output := ev.VerifyExpressions(ctx, input)
	assert.Nil(t, output.Err)
	assert.Equal(t, len(output.ExpressionVerificationOutputList), 1)
	assert.True(t, output.ExpressionVerificationOutputList[0].Result)
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}

// ReadSessionFile reads a session JSON file and
// unmarshal it's content into *internal.Conv.
func ReadSessionFile(conv *internal.Conv, sessionJSON string) error {
	s, err := os.ReadFile(sessionJSON)
	if err != nil {
		return err
	}
	err = json.Unmarshal(s, &conv)
	if err != nil {
		return err
	}
	return nil
}