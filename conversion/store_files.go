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

package conversion

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
)

// WriteSchemaFile writes DDL statements in a file. It includes CREATE TABLE
// statements and ALTER TABLE statements to add foreign keys.
// The parameter name should end with a .txt.
func WriteSchemaFile(conv *internal.Conv, now time.Time, name string, out *os.File, driver string) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create schema file %s: %v\n", name, err)
		return
	}

	// The schema file we write out below is optimized for reading. It includes comments, foreign keys
	// and doesn't add backticks around table and column names. This file is
	// intended for explanatory and documentation purposes, and is not strictly
	// legal Cloud Spanner DDL (Cloud Spanner doesn't currently support comments).
	spDDL := ddl.GetDDL(ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver}, conv.SpSchema, conv.SpSequences)
	if len(spDDL) == 0 {
		spDDL = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l := []string{
		fmt.Sprintf("-- Schema generated %s\n", now.Format("2006-01-02 15:04:05")),
		strings.Join(spDDL, ";\n\n"),
		"\n",
	}
	if _, err := f.WriteString(strings.Join(l, "")); err != nil {
		fmt.Fprintf(out, "Can't write out schema file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote schema to file '%s'.\n", name)

	// Convert <file_name>.<ext> to <file_name>.ddl.<ext>.
	nameSplit := strings.Split(name, ".")
	nameSplit = append(nameSplit[:len(nameSplit)-1], "ddl", nameSplit[len(nameSplit)-1])
	name = strings.Join(nameSplit, ".")
	f, err = os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create legal schema ddl file %s: %v\n", name, err)
		return
	}

	// We change 'Comments' to false and 'ProtectIds' to true below to write out a
	// schema file that is a legal Cloud Spanner DDL.
	spDDL = ddl.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver}, conv.SpSchema, conv.SpSequences)
	if len(spDDL) == 0 {
		spDDL = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l = []string{
		strings.Join(spDDL, ";\n\n"),
		"\n",
	}
	if _, err = f.WriteString(strings.Join(l, "")); err != nil {
		fmt.Fprintf(out, "Can't write out legal schema ddl file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote legal schema ddl to file '%s'.\n", name)
}

// WriteSessionFile writes conv struct to a file in JSON format.
func WriteSessionFile(conv *internal.Conv, name string, out *os.File) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create session file %s: %v\n", name, err)
		return
	}
	// Session file will basically contain 'conv' struct in JSON format.
	// It contains all the information for schema and data conversion state.
	convJSON, err := json.MarshalIndent(conv, "", " ")
	if err != nil {
		fmt.Fprintf(out, "Can't encode session state to JSON: %v\n", err)
		return
	}
	if _, err := f.Write(convJSON); err != nil {
		fmt.Fprintf(out, "Can't write out session file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote session to file '%s'.\n", name)
}

// WriteConvGeneratedFiles creates a directory labeled downloads with the current timestamp
// where it writes the sessionfile, report summary and DDLs then returns the directory where it writes.
func WriteConvGeneratedFiles(conv *internal.Conv, dbName string, driver string, BytesRead int64, out *os.File) (string, error) {
	now := time.Now()
	dirPath := "spanner_migration_tool_output/" + dbName + "/"
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		fmt.Fprintf(out, "Can't create directory %s: %v\n", dirPath, err)
		return "", err
	}
	schemaFileName := dirPath + dbName + "_schema.txt"
	WriteSchemaFile(conv, now, schemaFileName, out, driver)
	reportFileName := dirPath + dbName
	reportImpl := ReportImpl{}
	reportImpl.GenerateReport(driver, nil, BytesRead, "", conv, reportFileName, dbName, out)
	sessionFileName := dirPath + dbName + ".session.json"
	WriteSessionFile(conv, sessionFileName, out)
	return dirPath, nil
}

// ReadSessionFile reads a session JSON file and
// unmarshal it's content into *internal.Conv.
func ReadSessionFile(conv *internal.Conv, sessionJSON string) error {
	s, err := ioutil.ReadFile(sessionJSON)
	if err != nil {
		return err
	}
	err = json.Unmarshal(s, &conv)
	if err != nil {
		return err
	}
	return nil
}

// WriteBadData prints summary stats about bad rows and writes detailed info
// to file 'name'.
func WriteBadData(bw *writer.BatchWriter, conv *internal.Conv, banner, name string, out *os.File) {
	badConversions := conv.BadRows()
	badWrites := utils.SumMapValues(bw.DroppedRowsByTable())

	badDataStreaming := int64(0)
	if conv.Audit.StreamingStats.Streaming {
		badDataStreaming = getBadStreamingDataCount(conv)
	}

	if badConversions == 0 && badWrites == 0 && badDataStreaming == 0 {
		os.Remove(name) // Cleanup bad-data file from previous run.
		return
	}
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
		return
	}
	f.WriteString(banner)
	maxRows := 100
	if badConversions > 0 {
		l := conv.SampleBadRows(maxRows)
		if int64(len(l)) < badConversions {
			f.WriteString("A sample of rows that generated conversion errors:\n")
		} else {
			f.WriteString("Rows that generated conversion errors:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	if badWrites > 0 {
		l := bw.SampleBadRows(maxRows)
		if int64(len(l)) < badWrites {
			f.WriteString("A sample of rows that successfully converted but couldn't be written to Spanner:\n")
		} else {
			f.WriteString("Rows that successfully converted but couldn't be written to Spanner:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	if badDataStreaming > 0 {
		err = writeBadStreamingData(conv, f)
		if err != nil {
			fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
			return
		}
	}

	fmt.Fprintf(out, "See file '%s' for details of bad rows\n", name)
}

// writeBadStreamingData writes sample of bad records and dropped records during streaming
// migration process to bad data file.
func writeBadStreamingData(conv *internal.Conv, f *os.File) error {
	f.WriteString("\nBad data encountered during streaming migration:\n\n")

	stats := (conv.Audit.StreamingStats)

	badRecords := int64(0)
	for _, x := range stats.BadRecords {
		badRecords += utils.SumMapValues(x)
	}
	droppedRecords := int64(0)
	for _, x := range stats.DroppedRecords {
		droppedRecords += utils.SumMapValues(x)
	}

	if badRecords > 0 {
		l := stats.SampleBadRecords
		if int64(len(l)) < badRecords {
			f.WriteString("A sample of records that generated conversion errors:\n")
		} else {
			f.WriteString("Records that generated conversion errors:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				return err
			}
		}
		f.WriteString("\n")
	}
	if droppedRecords > 0 {
		l := stats.SampleBadWrites
		if int64(len(l)) < droppedRecords {
			f.WriteString("A sample of records that successfully converted but couldn't be written to Spanner:\n")
		} else {
			f.WriteString("Records that successfully converted but couldn't be written to Spanner:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// getBadStreamingDataCount returns the total sum of bad and dropped records during
// streaming migration process.
func getBadStreamingDataCount(conv *internal.Conv) int64 {
	badDataCount := int64(0)

	for _, x := range conv.Audit.StreamingStats.BadRecords {
		badDataCount += utils.SumMapValues(x)
	}
	for _, x := range conv.Audit.StreamingStats.DroppedRecords {
		badDataCount += utils.SumMapValues(x)
	}
	return badDataCount
}
