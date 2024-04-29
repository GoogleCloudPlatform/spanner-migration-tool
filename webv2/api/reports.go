package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

type ReportAPIHandler struct {
	Report          conversion.ReportInterface
	ReportGenerator reports.ReportInterface
}

// getReportFile generates report file and returns file path.
func (reportHandler *ReportAPIHandler) GetReportFile(w http.ResponseWriter, r *http.Request) {
	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	var err error
	now := time.Now()
	filePrefix, err := utilities.GetFilePrefix(now)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not get file prefix : %v", err), http.StatusInternalServerError)
	}
	reportFileName := "frontend/" + filePrefix
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	reportHandler.Report.GenerateReport(sessionState.Driver, nil, ioHelper.BytesRead, "", sessionState.Conv, reportFileName, sessionState.DbName, ioHelper.Out)
	reportAbsPath, err := filepath.Abs(reportFileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not create absolute path : %v", err), http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(reportAbsPath))
}

// generates a downloadable structured report and send it as a JSON response
func (reportHandler *ReportAPIHandler) GetDStructuredReport(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	structuredReport := reportHandler.ReportGenerator.GenerateStructuredReport(sessionState.Driver, sessionState.DbName, sessionState.Conv, nil, true, true)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(structuredReport)
}

// generates a downloadable text report and send it as a JSON response
func (reportHandler *ReportAPIHandler) GetDTextReport(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	structuredReport := reportHandler.ReportGenerator.GenerateStructuredReport(sessionState.Driver, sessionState.DbName, sessionState.Conv, nil, true, true)
	// creates a new buffer
	buffer := bytes.NewBuffer([]byte{})
	// initializes buffered writer that writes data to buffer
	wb := bufio.NewWriter(buffer)
	reportHandler.ReportGenerator.GenerateTextReport(structuredReport, wb)
	// flushes buffered data to writer
	wb.Flush()
	// introduces a byte slice to represent the content of buffer
	data := buffer.Bytes()
	// converts byte slice to corressponding string representation
	decodedString := string(data)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain")
	json.NewEncoder(w).Encode(decodedString)
}

// generates a downloadable DDL(spanner) and send it as a JSON response
func GetDSpannerDDL(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.RLock()
	defer sessionState.Conv.ConvLock.RUnlock()
	conv := sessionState.Conv
	now := time.Now()
	spDDL := ddl.GetDDL(ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: sessionState.Driver}, conv.SpSchema, conv.SpSequences)
	if len(spDDL) == 0 {
		spDDL = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l := []string{
		fmt.Sprintf("-- Schema generated %s\n", now.Format("2006-01-02 15:04:05")),
		strings.Join(spDDL, ";\n\n"),
		"\n",
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(strings.Join(l, ""))
}
