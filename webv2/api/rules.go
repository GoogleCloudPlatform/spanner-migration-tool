package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/index"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/primarykey"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/table"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/types"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

// ApplyRule allows to add rules that changes the schema
// currently it supports two types of operations viz. SetGlobalDataType and AddIndex
func ApplyRule(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var rule internal.Rule
	err = json.Unmarshal(reqBody, &rule)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	if rule.Type == constants.GlobalDataTypeChange {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		typeMap := map[string]string{}
		err = json.Unmarshal(d, &typeMap)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		setGlobalDataType(typeMap)
	} else if rule.Type == constants.AddIndex {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		newIdx := ddl.CreateIndex{}
		err = json.Unmarshal(d, &newIdx)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		addedIndex, err := addIndex(newIdx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rule.Data = addedIndex
	} else if rule.Type == constants.EditColumnMaxLength {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		var colMaxLength types.ColMaxLength
		err = json.Unmarshal(d, &colMaxLength)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		setSpColMaxLength(colMaxLength, rule.AssociatedObjects)
	} else if rule.Type == constants.AddShardIdPrimaryKey {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		var shardIdPrimaryKey types.ShardIdPrimaryKey
		err = json.Unmarshal(d, &shardIdPrimaryKey)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		tableName := checkInterleaving()
		if tableName != "" {
			http.Error(w, fmt.Sprintf("Rule cannot be added because some tables, eg: %v are interleaved. Please remove interleaving and try again.", tableName), http.StatusBadRequest)
			return
		}
		setShardIdColumnAsPrimaryKey(shardIdPrimaryKey.AddedAtTheStart)
		addShardIdColumnToForeignKeys(shardIdPrimaryKey.AddedAtTheStart)
	} else {
		http.Error(w, "Invalid rule type", http.StatusInternalServerError)
		return
	}

	ruleId := internal.GenerateRuleId()
	rule.Id = ruleId

	sessionState.Conv.Rules = append(sessionState.Conv.Rules, rule)
	session.UpdateSessionFile()
	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func DropRule(w http.ResponseWriter, r *http.Request) {
	ruleId := r.FormValue("id")
	if ruleId == "" {
		http.Error(w, fmt.Sprint("Rule id is empty"), http.StatusBadRequest)
		return
	}
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	conv := sessionState.Conv
	var rule internal.Rule
	position := -1

	for i, r := range conv.Rules {
		if r.Id == ruleId {
			rule = r
			position = i
			break
		}
	}
	if position == -1 {
		http.Error(w, fmt.Sprint("Rule to be deleted not found"), http.StatusBadRequest)
		return
	}

	if rule.Type == constants.AddIndex {
		if rule.Enabled {
			d, err := json.Marshal(rule.Data)
			if err != nil {
				http.Error(w, "Invalid rule data", http.StatusInternalServerError)
				return
			}
			var index ddl.CreateIndex
			err = json.Unmarshal(d, &index)
			if err != nil {
				http.Error(w, "Invalid rule data", http.StatusInternalServerError)
				return
			}
			tableId := index.TableId
			indexId := index.Id
			err = dropSecondaryIndexHelper(tableId, indexId)
			if err != nil {
				http.Error(w, fmt.Sprintf("%v", err), http.StatusBadRequest)
				return
			}
		}
	} else if rule.Type == constants.GlobalDataTypeChange {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		typeMap := map[string]string{}
		err = json.Unmarshal(d, &typeMap)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		revertGlobalDataType(typeMap)
	} else if rule.Type == constants.EditColumnMaxLength {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		var colMaxLength types.ColMaxLength
		err = json.Unmarshal(d, &colMaxLength)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		revertSpColMaxLength(colMaxLength, rule.AssociatedObjects)
	} else if rule.Type == constants.AddShardIdPrimaryKey {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		var shardIdPrimaryKey types.ShardIdPrimaryKey
		err = json.Unmarshal(d, &shardIdPrimaryKey)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		tableName := checkInterleaving()
		if tableName != "" {
			http.Error(w, fmt.Sprintf("Rule cannot be deleted because some tables, eg: %v are interleaved. Please remove interleaving and try again.", tableName), http.StatusBadRequest)
			return
		}
		revertShardIdColumnAsPrimaryKey(shardIdPrimaryKey.AddedAtTheStart)
		removeShardIdColumnFromForeignKeys(shardIdPrimaryKey.AddedAtTheStart)
	} else {
		http.Error(w, "Invalid rule type", http.StatusInternalServerError)
		return
	}

	sessionState.Conv.Rules = append(conv.Rules[:position], conv.Rules[position+1:]...)
	if len(sessionState.Conv.Rules) == 0 {
		sessionState.Conv.Rules = nil
	}
	session.UpdateSessionFile()
	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

// setGlobalDataType allows to change Spanner type globally.
// It takes a map from source type to Spanner type and updates
// the Spanner schema accordingly.
func setGlobalDataType(typeMap map[string]string) {
	sessionState := session.GetSessionState()

	// Redo source-to-Spanner typeMap using t (the mapping specified in the http request).
	// We drive this process by iterating over the Spanner schema because we want to preserve all
	// other customizations that have been performed via the UI (dropping columns, renaming columns
	// etc). In particular, note that we can't just blindly redo schema conversion (using an appropriate
	// version of 'toDDL' with the new typeMap).
	for tableId, spSchema := range sessionState.Conv.SpSchema {
		for colId := range spSchema.ColDefs {
			srcColDef := sessionState.Conv.SrcSchema[tableId].ColDefs[colId]
			// If the srcCol's type is in the map, then recalculate the Spanner type
			// for this column using the map. Otherwise, leave the ColDef for this
			// column as is. Note that per-column type overrides could be lost in
			// this process -- the mapping in typeMap always takes precendence.
			if _, found := typeMap[srcColDef.Type.Name]; found {
				utilities.UpdateDataType(sessionState.Conv, typeMap[srcColDef.Type.Name], tableId, colId)
			}
		}
		common.ComputeNonKeyColumnSize(sessionState.Conv, tableId)
	}
}

// addIndex checks the new name for spanner name validity, ensures the new name is already not used by existing tables
// secondary indexes or foreign key constraints. If above checks passed then new indexes are added to the schema else appropriate
// error thrown.
func addIndex(newIndex ddl.CreateIndex) (ddl.CreateIndex, error) {
	// Check new name for spanner name validity.
	newNames := []string{}
	newNames = append(newNames, newIndex.Name)

	if ok, invalidNames := utilities.CheckSpannerNamesValidity(newNames); !ok {
		return ddl.CreateIndex{}, fmt.Errorf("following names are not valid Spanner identifiers: %s", strings.Join(invalidNames, ","))
	}
	// Check that the new names are not already used by existing tables, secondary indexes or foreign key constraints.
	if ok, err := utilities.CanRename(newNames, newIndex.TableId); !ok {
		return ddl.CreateIndex{}, err
	}

	sessionState := session.GetSessionState()
	sp := sessionState.Conv.SpSchema[newIndex.TableId]

	newIndexes := []ddl.CreateIndex{newIndex}
	index.CheckIndexSuggestion(newIndexes, sp)
	for i := 0; i < len(newIndexes); i++ {
		newIndexes[i].Id = internal.GenerateIndexesId()
	}

	sessionState.Conv.UsedNames[strings.ToLower(newIndex.Name)] = true
	sp.Indexes = append(sp.Indexes, newIndexes...)
	sessionState.Conv.SpSchema[newIndex.TableId] = sp
	return newIndexes[0], nil
}

func setSpColMaxLength(spColMaxLength types.ColMaxLength, associatedObjects string) {
	sessionState := session.GetSessionState()
	if associatedObjects == "All table" {
		for tId := range sessionState.Conv.SpSchema {
			for _, colDef := range sessionState.Conv.SpSchema[tId].ColDefs {
				if colDef.T.Name == spColMaxLength.SpDataType {
					spColDef := colDef
					if spColDef.T.Len == ddl.MaxLength {
						spColDef.T.Len, _ = strconv.ParseInt(spColMaxLength.SpColMaxLength, 10, 64)
					}
					sessionState.Conv.SpSchema[tId].ColDefs[colDef.Id] = spColDef
				}
			}
			common.ComputeNonKeyColumnSize(sessionState.Conv, tId)
		}
	} else {
		for _, colDef := range sessionState.Conv.SpSchema[associatedObjects].ColDefs {
			if colDef.T.Name == spColMaxLength.SpDataType {
				spColDef := colDef
				if spColDef.T.Len == ddl.MaxLength {
					table.UpdateColumnSize(spColMaxLength.SpColMaxLength, associatedObjects, colDef.Id, sessionState.Conv)
				}
			}
		}
		common.ComputeNonKeyColumnSize(sessionState.Conv, associatedObjects)
	}
}

func revertSpColMaxLength(spColMaxLength types.ColMaxLength, associatedObjects string) {
	sessionState := session.GetSessionState()
	spColLen, _ := strconv.ParseInt(spColMaxLength.SpColMaxLength, 10, 64)
	if associatedObjects == "All tables" {
		for tId := range sessionState.Conv.SpSchema {
			for colId, colDef := range sessionState.Conv.SpSchema[tId].ColDefs {
				if colDef.T.Name == spColMaxLength.SpDataType {
					utilities.UpdateMaxColumnLen(sessionState.Conv, spColMaxLength.SpDataType, tId, colId, spColLen)
				}
			}
			common.ComputeNonKeyColumnSize(sessionState.Conv, tId)
		}
	} else {
		for colId, colDef := range sessionState.Conv.SpSchema[associatedObjects].ColDefs {
			if colDef.T.Name == spColMaxLength.SpDataType {
				utilities.UpdateMaxColumnLen(sessionState.Conv, spColMaxLength.SpDataType, associatedObjects, colId, spColLen)
			}
		}
		common.ComputeNonKeyColumnSize(sessionState.Conv, associatedObjects)
	}
}

// revertGlobalDataType revert back the spanner type to default
// when the rule that is used to apply the data-type change is deleted.
// It takes a map from source type to Spanner type and updates
// the Spanner schema accordingly.
func revertGlobalDataType(typeMap map[string]string) {
	sessionState := session.GetSessionState()

	for tableId, spSchema := range sessionState.Conv.SpSchema {
		for colId, colDef := range spSchema.ColDefs {
			srcColDef, found := sessionState.Conv.SrcSchema[tableId].ColDefs[colId]
			if !found {
				continue
			}
			spType, found := typeMap[srcColDef.Type.Name]

			if !found {
				continue
			}

			if colDef.T.Name == spType {
				utilities.UpdateDataType(sessionState.Conv, "", tableId, colId)
			}
		}
		common.ComputeNonKeyColumnSize(sessionState.Conv, tableId)
	}
}

func removeShardIdColumnFromForeignKeys(isAddedAtFirst bool) {
	sessionState := session.GetSessionState()
	for tableId, table := range sessionState.Conv.SpSchema {
		for i, fk := range table.ForeignKeys {

			if isAddedAtFirst {
				fk.ColIds = fk.ColIds[1:]
				fk.ReferColumnIds = fk.ReferColumnIds[1:]
			} else {
				fk.ColIds = fk.ColIds[:len(fk.ColIds)-1]
				fk.ReferColumnIds = fk.ReferColumnIds[:len(fk.ReferColumnIds)-1]
			}
			sessionState.Conv.SpSchema[tableId].ForeignKeys[i] = fk
		}
	}
}

func revertShardIdColumnAsPrimaryKey(isAddedAtFirst bool) {
	sessionState := session.GetSessionState()
	for _, table := range sessionState.Conv.SpSchema {
		pkRequest := primarykey.PrimaryKeyRequest{
			TableId: table.Id,
			Columns: []ddl.IndexKey{},
		}
		for index := range table.PrimaryKeys {
			pk := table.PrimaryKeys[index]
			if pk.ColId != table.ShardIdColumn {
				decrement := 0
				if isAddedAtFirst {
					decrement = 1
				}
				pkRequest.Columns = append(pkRequest.Columns, ddl.IndexKey{ColId: pk.ColId, Order: pk.Order - decrement, Desc: pk.Desc})
			}
		}
		primarykey.UpdatePrimaryKeyAndSessionFile(pkRequest)
	}
}

func checkInterleaving() string {
	sessionState := session.GetSessionState()
	for _, spSchema := range sessionState.Conv.SpSchema {
		if spSchema.ParentTable.Id != "" {
			return spSchema.Name
		}
	}
	return ""
}
