package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

func AddNewSequence(w http.ResponseWriter, r *http.Request) {
	fmt.Println("request started", "method", r.Method, "path", r.URL.Path)
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("request's body Read Error")
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}
	seq := ddl.Sequence{}
	err = json.Unmarshal(reqBody, &seq)
	if err != nil {
		fmt.Println("request's Body parse error")
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()

	if ok, _ := utilities.CheckSpannerNamesValidity([]string{seq.Name}); !ok {
		http.Error(w, fmt.Sprintf("Sequence Name is not valid: %v", seq.Name), http.StatusBadRequest)
		return
	}

	// Check that the new names are not already used by existing tables, secondary indexes, sequence or foreign key constraints.
	if ok, err := utilities.CanRename([]string{seq.Name}, ""); !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	spSequences := sessionState.Conv.SpSequences
	seq.Id = internal.GenerateSequenceId()
	sessionState.Conv.UsedNames[strings.ToLower(seq.Name)] = true

	spSequences[seq.Name] = seq
	sessionState.Conv.SpSequences = spSequences

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func UpdateSequence(w http.ResponseWriter, r *http.Request) {
	fmt.Println("request started", "method", r.Method, "path", r.URL.Path)
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("request's body Read Error")
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}
	newSeq := ddl.Sequence{}
	err = json.Unmarshal(reqBody, &newSeq)
	if err != nil {
		fmt.Println("request's Body parse error")
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	spSequences := sessionState.Conv.SpSequences

	for i, seq := range spSequences {

		if seq.Id == newSeq.Id {
			spSequences[i] = newSeq
			break
		}
	}

	sessionState.Conv.SpSequences = spSequences

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func DropSequence(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	var dropDetail struct{ SequenceName string }
	if err = json.Unmarshal(reqBody, &dropDetail); err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, "Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner.", http.StatusNotFound)
		return
	}

	if dropDetail.SequenceName == "" {
		http.Error(w, "Sequence name is empty", http.StatusBadRequest)
	}

	spSequence := sessionState.Conv.SpSequences
	updatedTables := dropSequenceHelper(spSequence[dropDetail.SequenceName].ColumnsUsingSeq, sessionState.Conv.SpSchema)
	sessionState.Conv.SpSchema = updatedTables

	delete(spSequence, dropDetail.SequenceName)
	sessionState.Conv.SpSequences = spSequence

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func dropSequenceHelper(columnsUsingSeq map[string][]string, tables ddl.Schema) ddl.Schema {
	for tableName, columns := range columnsUsingSeq {
		for _, colId := range columns {
			columnDef := tables[tableName].ColDefs[colId]
			columnDef.AutoGen = ddl.AutoGenCol{}
			tables[tableName].ColDefs[colId] = columnDef
		}
	}
	return tables
}

func GetSequenceDDL(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()

	seqDDL := make(map[string]string)
	for seqName, seq := range sessionState.Conv.SpSequences {
		var ddl string
		switch sessionState.Dialect {
		case constants.POSTGRES:
			ddl = seq.PGPrintSequence()
		default:
			ddl = seq.PrintSequence()
		}
		seqDDL[seqName] = ddl
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(seqDDL)
}

func GetSequenceKind(w http.ResponseWriter, r *http.Request) {
	sequenceKind := []string{
		"BIT REVERSED POSITIVE",
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sequenceKind)
}
