package api_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/api"
	"github.com/stretchr/testify/assert"
)

type ReportMock struct{}

func (r *ReportMock) GenerateReport(driver string, badWrites map[string]int64, BytesRead int64, banner string, conv *internal.Conv, reportFileName string, dbName string, out *os.File) {
	// do nothing since we don't want to test report generation here, only the API.
}

type GenerateReportMock struct{}

func (r *GenerateReportMock) GenerateTextReport(structuredReport reports.StructuredReport, w *bufio.Writer) {
	// do nothing since we don't want to test report generation here, only the API.
}

func (r *GenerateReportMock) GenerateStructuredReport(driverName string, dbName string, conv *internal.Conv, badWrites map[string]int64, printTableReports bool, printUnexpecteds bool) reports.StructuredReport {
	return reports.StructuredReport{
		MigrationType: "test",
	}
}

func TestGetReportFile(t *testing.T) {
	reportAPIHandler := api.ReportAPIHandler{
		Report: &ReportMock{},
	}
	req, err := http.NewRequest("POST", "/report", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(reportAPIHandler.GetReportFile)
	handler.ServeHTTP(rr, req)
	//API generates a report file and returns a file path
	reportFilePath := string(rr.Body.String())
	assert.Contains(t, reportFilePath, "frontend")
}

func TestGetDStructuredReport(t *testing.T) {
	reportAPIHandler := api.ReportAPIHandler{
		ReportGenerator: &GenerateReportMock{},
	}
	req, err := http.NewRequest("POST", "/downloadStructuredReport", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(reportAPIHandler.GetDStructuredReport)
	handler.ServeHTTP(rr, req)
	//API generates a structured report JSON and returns it
	var structuredReport reports.StructuredReport
	json.Unmarshal(rr.Body.Bytes(), &structuredReport)
	assert.Equal(t, structuredReport.MigrationType, "test")
}

func TestGetDTextReport(t *testing.T) {
	reportAPIHandler := api.ReportAPIHandler{
		ReportGenerator: &GenerateReportMock{},
	}
	req, err := http.NewRequest("POST", "/downloadTextReport", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(reportAPIHandler.GetDTextReport)
	handler.ServeHTTP(rr, req)
	//API generates a text report string and returns it
	textReport := string(rr.Body.String())
	assert.NotNil(t, textReport)
}

func TestGetDSpannerDDL(t *testing.T) {
	req, err := http.NewRequest("POST", "/downloadTextReport", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.GetDSpannerDDL)
	handler.ServeHTTP(rr, req)
	assert.Contains(t, rr.Body.String(), "Schema generated")
	assert.Contains(t, rr.Body.String(), "Schema is empty")
	assert.Contains(t, rr.Body.String(), "no tables found")
}
