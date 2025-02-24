package cmd

import (
        "flag"
        "testing"

        "github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
        "github.com/stretchr/testify/assert"
)

func TestSchemaAndDataSetFlags(t *testing.T) {
        testCases:=struct {
                testName       string
                flagArgs      string
                expectedValues SchemaAndDataCmd
        }{
                {
                        testName: "Default Values",
                        expectedValues: SchemaAndDataCmd{
                                source:           "",
                                sourceProfile:    "",
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           false,
                                logLevel:         "DEBUG",
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "Non-Default Values",
                        flagArgs:string{
                                "-source=MySQL",
                                "-source-profile=file=test.sql",
                                "-target=Spanner",
                                "-target-profile=instance=test-instance",
                                "-prefix=test-prefix",
                                "-write-limit=1000",
                                "-dry-run",
                                "-log-level=INFO",
                                "-skip-foreign-keys",
                                "-validate",
                        },
                        expectedValues: SchemaAndDataCmd{
                                source:           "MySQL",
                                sourceProfile:    "file=test.sql",
                                target:           "Spanner",
                                targetProfile:    "instance=test-instance",
                                filePrefix:       "test-prefix",
                                WriteLimit:       1000,
                                dryRun:           true,
                                logLevel:         "INFO",
                                SkipForeignKeys:  true,
                                validate:         true,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "Invalid Log Level",
                        flagArgs:string{
                                "-log-level=INVALID",
                        },
                        expectedValues: SchemaAndDataCmd{
                                source:           "",
                                sourceProfile:    "",
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           false,
                                logLevel:         "DEBUG", // Should remain at the default value
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "Empty Source Profile",
                        flagArgs:string{
                                "-source=MySQL",
                                "-source-profile=",
                        },
                        expectedValues: SchemaAndDataCmd{
                                source:           "MySQL",
                                sourceProfile:    "", // Should be an empty string
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           false,
                                logLevel:         "DEBUG",
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
        }

        for _, tc:= range testCases { // Now it can range over testCases
                t.Run(tc.testName, func(t *testing.T) {
                        fs:= flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
                        fs.Parse(tc.flagArgs)

                        schemaAndDataCmd:= SchemaAndDataCmd{}
                        schemaAndDataCmd.SetFlags(fs)
                        assert.Equal(t, tc.expectedValues, schemaAndDataCmd, tc.testName)
                })
        }
}
