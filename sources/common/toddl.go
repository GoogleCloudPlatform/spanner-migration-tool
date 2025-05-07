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

/*
Package common creates an outline for common functionality across the multiple
source databases we support.
While adding new methods or code here
 1. Ensure that the changes do not adversely impact any source that uses the
    common code
 2. Test cases might not sufficiently cover all cases, so integration and
    manual testing should be done ensure no functionality is breaking. Most of
    the test cases that cover the code in this package will lie in the
    implementing source databases, so it might not be required to have unit
    tests for each method here.
 3. Any functions added here should be used by two or more databases
 4. If it looks like the code is getting more complex due to refactoring,
    it is probably better off leaving the functionality out of common
*/
package common

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

// ToDdl interface is meant to be implemented by all sources. When support for a
// new target database is added, please add a new method here with the output
// type expected. In case a particular source to target transoformation is not
// supported, an error is to be returned by the corresponding method.
type ToDdl interface {
	ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type, isPk bool) (ddl.Type, []internal.SchemaIssue)
	GetColumnAutoGen(conv *internal.Conv, autoGenCol ddl.AutoGenCol, colId string, tableId string) (*ddl.AutoGenCol, error)
}

type SchemaToSpannerInterface interface {
	SchemaToSpannerDDL(conv *internal.Conv, toddl ToDdl, attributes internal.AdditionalSchemaAttributes) error
	SchemaToSpannerDDLHelper(conv *internal.Conv, toddl ToDdl, srcTable schema.Table, isRestore bool) error
	SchemaToSpannerSequenceHelper(conv *internal.Conv, srcSequence ddl.Sequence) error
}

type SchemaToSpannerImpl struct {
	ExpressionVerificationAccessor expressions_api.ExpressionVerificationAccessor
	DdlV                           expressions_api.DDLVerifier
}

var ErrorTypeMapping = map[string]internal.SchemaIssue{
	"No matching signature for operator": internal.TypeMismatchError,
	"Syntax error":                       internal.InvalidConditionError,
	"Unrecognized name":                  internal.ColumnNotFoundError,
	"Function not found":                 internal.CheckConstraintFunctionNotFoundError,
	"unhandled error":                    internal.GenericError,
}

// SchemaToSpannerDDL performs schema conversion from the source DB schema to
// Spanner. It uses the source schema in conv.SrcSchema, and writes
// the Spanner schema to conv.SpSchema.
func (ss *SchemaToSpannerImpl) SchemaToSpannerDDL(conv *internal.Conv, toddl ToDdl, attributes internal.AdditionalSchemaAttributes) error {
	srcSequences := conv.SrcSequences
	for _, srcSequence := range srcSequences {
		ss.SchemaToSpannerSequenceHelper(conv, srcSequence)
	}
	tableIds := GetSortedTableIdsBySrcName(conv.SrcSchema)
	for _, tableId := range tableIds {
		srcTable := conv.SrcSchema[tableId]
		ss.SchemaToSpannerDDLHelper(conv, toddl, srcTable, false)
	}

	conv.AddPrimaryKeys()
	if attributes.IsSharded {
		conv.AddShardIdColumn()
	}

	if conv.Source == constants.MYSQL && conv.SpProjectId != "" && conv.SpInstanceId != "" {
		// Process and verify Spanner DDL expressions for MYSQL
		expressionDetails := ss.DdlV.GetSourceExpressionDetails(conv, tableIds)
		expressions, err := ss.DdlV.VerifySpannerDDL(conv, expressionDetails)
		if err != nil && !strings.Contains(err.Error(), "expressions either failed verification") {
			return err
		}
		spannerSchemaApplyExpressions(conv, expressions)
	}

	if (conv.Source == constants.MYSQL || conv.Source == constants.MYSQLDUMP) && conv.SpProjectId != "" && conv.SpInstanceId != "" {
		if ss.ExpressionVerificationAccessor != nil {
			// Process and verify Check constraints for MySQL and MySQLDump flow only
			err := ss.VerifyExpressions(conv)
			if err != nil {
				return err
			}
		}
	}

	internal.ResolveRefs(conv)
	return nil
}

// GenerateExpressionDetailList it will generate the expression detail list which is used in verify expression method as a input
func GenerateExpressionDetailList(spschema ddl.Schema) []internal.ExpressionDetail {
	expressionDetailList := []internal.ExpressionDetail{}
	for _, sp := range spschema {
		for _, cc := range sp.CheckConstraints {

			expressionDetail := internal.ExpressionDetail{
				Expression:       cc.Expr,
				Type:             "CHECK",
				ReferenceElement: internal.ReferenceElement{Name: sp.Name},
				ExpressionId:     cc.ExprId,
				Metadata:         map[string]string{"tableId": sp.Id},
			}
			expressionDetailList = append(expressionDetailList, expressionDetail)
		}
	}

	return expressionDetailList
}

// RemoveError it will reset the table issue before re-populating
func RemoveError(tableIssues map[string]internal.TableIssues) map[string]internal.TableIssues {

	for tableId, TableIssues := range tableIssues {
		for _, issue := range ErrorTypeMapping {
			removedIssue := removeSchemaIssue(TableIssues.TableLevelIssues, issue)
			TableIssues.TableLevelIssues = removedIssue
			tableIssues[tableId] = TableIssues
		}

	}
	return tableIssues

}

// GetIssue it will collect all the error and return it
func GetIssue(result internal.VerifyExpressionsOutput) (map[string][]internal.InvalidCheckExp, map[string][]string) {
	exprOutputsByTable := make(map[string][]internal.ExpressionVerificationOutput)
	issues := make(map[string][]internal.InvalidCheckExp)
	invalidExpIds := make(map[string][]string)
	for _, ev := range result.ExpressionVerificationOutputList {
		if !ev.Result {
			tableId := ev.ExpressionDetail.Metadata["tableId"]
			exprOutputsByTable[tableId] = append(exprOutputsByTable[tableId], ev)
		}
	}

	for tableId, exprOutputs := range exprOutputsByTable {

		for _, ev := range exprOutputs {
			var issue internal.SchemaIssue

			switch {
			case strings.Contains(ev.Err.Error(), "No matching signature for operator"):
				issue = internal.TypeMismatch
			case strings.Contains(ev.Err.Error(), "Syntax error") || strings.Contains(ev.Err.Error(), "syntax error at or near"):
				issue = internal.InvalidCondition
			case strings.Contains(ev.Err.Error(), "Unrecognized name"):
				issue = internal.ColumnNotFound
			case strings.Contains(ev.Err.Error(), "Function not found"):
				issue = internal.CheckConstraintFunctionNotFound
			default:
				issue = internal.GenericWarning
			}
			issues[tableId] = append(issues[tableId], internal.InvalidCheckExp{
				IssueType:  issue,
				Expression: ev.ExpressionDetail.Expression,
			})
			invalidExpIds[tableId] = append(invalidExpIds[tableId], ev.ExpressionDetail.ExpressionId)

		}

	}

	return issues, invalidExpIds

}

// GetErroredIssue it will collect all the error and return it
func GetErroredIssue(result internal.VerifyExpressionsOutput) map[string][]internal.InvalidCheckExp {
	exprOutputsByTable := make(map[string][]internal.ExpressionVerificationOutput)
	issues := make(map[string][]internal.InvalidCheckExp)
	for _, ev := range result.ExpressionVerificationOutputList {
		if !ev.Result {
			tableId := ev.ExpressionDetail.Metadata["tableId"]
			exprOutputsByTable[tableId] = append(exprOutputsByTable[tableId], ev)
		}
	}

	for tableId, exprOutputs := range exprOutputsByTable {

		for _, ev := range exprOutputs {
			var issue internal.SchemaIssue

			switch {
			case strings.Contains(ev.Err.Error(), "No matching signature for operator"):
				issue = internal.TypeMismatchError
			case strings.Contains(ev.Err.Error(), "Syntax error") || strings.Contains(ev.Err.Error(), "syntax error at or near"):
				issue = internal.InvalidConditionError
			case strings.Contains(ev.Err.Error(), "Unrecognized name"):
				issue = internal.ColumnNotFoundError
			case strings.Contains(ev.Err.Error(), "Function not found"):
				issue = internal.CheckConstraintFunctionNotFoundError
			default:
				issue = internal.GenericError
			}
			issues[tableId] = append(issues[tableId], internal.InvalidCheckExp{
				IssueType:  issue,
				Expression: ev.ExpressionDetail.Expression,
			})

		}

	}

	return issues

}

// RemoveCheckConstraint this method will remove the constraint which has error
func RemoveCheckConstraint(checkConstraints []ddl.CheckConstraint, expId string) []ddl.CheckConstraint {
	var filteredConstraints []ddl.CheckConstraint

	for _, checkConstraint := range checkConstraints {
		if checkConstraint.ExprId != expId {
			filteredConstraints = append(filteredConstraints, checkConstraint)
		}
	}
	return filteredConstraints
}

// VerifyExpression this function will use expression_api to validate check constraint expressions and add the relevant error
// to suggestion tab and remove the check constraint which has error
func (ss *SchemaToSpannerImpl) VerifyExpressions(conv *internal.Conv) error {
	ctx := context.Background()

	spschema := conv.SpSchema

	verifyExpressionsInput := internal.VerifyExpressionsInput{
		Conv:                 conv,
		Source:               conv.Source,
		ExpressionDetailList: GenerateExpressionDetailList(spschema),
	}
	ss.ExpressionVerificationAccessor.RefreshSpannerClient(ctx, conv.SpProjectId, conv.SpInstanceId)
	if len(verifyExpressionsInput.ExpressionDetailList) != 0 {
		result := ss.ExpressionVerificationAccessor.VerifyExpressions(ctx, verifyExpressionsInput)
		if result.ExpressionVerificationOutputList == nil {
			return result.Err
		}
		issueTypes, invalidExpIds := GetIssue(result)
		if len(issueTypes) > 0 {
			for tableId, issues := range issueTypes {

				if conv.InvalidCheckExp == nil {
					conv.InvalidCheckExp = map[string][]internal.InvalidCheckExp{}
					conv.InvalidCheckExp[tableId] = []internal.InvalidCheckExp{}
				}

				invalidCheckExp := conv.InvalidCheckExp[tableId]
				invalidCheckExp = append(invalidCheckExp, issues...)
				conv.InvalidCheckExp[tableId] = invalidCheckExp

				for _, issue := range issues {
					if _, exists := conv.SchemaIssues[tableId]; !exists {
						conv.SchemaIssues[tableId] = internal.TableIssues{
							TableLevelIssues: []internal.SchemaIssue{},
						}
					}

					tableIssue := conv.SchemaIssues[tableId]

					if !IsSchemaIssuePresent(tableIssue.TableLevelIssues, issue.IssueType) {
						tableIssue.TableLevelIssues = append(tableIssue.TableLevelIssues, issue.IssueType)
					}
					conv.SchemaIssues[tableId] = tableIssue
				}
			}
		}

		if len(invalidExpIds) > 0 {
			for tableId, expressionIdList := range invalidExpIds {
				for _, expId := range expressionIdList {
					spschema := conv.SpSchema[tableId]
					spschema.CheckConstraints = RemoveCheckConstraint(spschema.CheckConstraints, expId)
					conv.SpSchema[tableId] = spschema
				}
			}
		}
	}

	return nil
}

// IsSchemaIssuePresent checks if issue is present in the given schemaissue list.
func IsSchemaIssuePresent(schemaissue []internal.SchemaIssue, issue internal.SchemaIssue) bool {

	for _, s := range schemaissue {
		if s == issue {
			return true
		}
	}
	return false
}

func (ss *SchemaToSpannerImpl) SchemaToSpannerDDLHelper(conv *internal.Conv, toddl ToDdl, srcTable schema.Table, isRestore bool) error {
	spTableName, err := internal.GetSpannerTable(conv, srcTable.Id)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't map source table %s to Spanner: %s", srcTable.Name, err))
		return err
	}
	var spColIds []string
	spColDef := make(map[string]ddl.ColumnDef)

	var (
		totalNonKeyColumnSize int
		tableLevelIssues      []internal.SchemaIssue
	)

	columnLevelIssues := make(map[string][]internal.SchemaIssue)

	// Iterate over columns using ColNames order.
	for _, srcColId := range srcTable.ColIds {
		srcCol := srcTable.ColDefs[srcColId]
		colName, err := internal.GetSpannerCol(conv, srcTable.Id, srcCol.Id, spColDef)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't map source column %s of table %s to Spanner: %s", srcTable.Name, srcCol.Name, err))
			continue
		}
		spColIds = append(spColIds, srcColId)
		isPk := IsPrimaryKey(srcColId, srcTable)
		ty, issues := toddl.ToSpannerType(conv, "", srcCol.Type, isPk)

		// TODO(hengfeng): add issues for all elements of srcCol.Ignored.
		if srcCol.Ignored.ForeignKey {
			issues = append(issues, internal.ForeignKey)
		}
		_, isChanged := internal.FixName(srcCol.Name)
		if isChanged && (srcCol.Name != colName) {
			issues = append(issues, internal.IllegalName)
		}
		if srcCol.Ignored.Default {
			issues = append(issues, internal.DefaultValue)
		}
		if srcCol.Ignored.AutoIncrement { // TODO(adibh) - check why this is not there in postgres
			issues = append(issues, internal.AutoIncrement)
		}
		// Set the not null constraint to false for unsupported source datatypes
		isNotNull := srcCol.NotNull
		if findSchemaIssue(issues, internal.NoGoodType) != -1 {
			isNotNull = false
		}
		// Set the not null constraint to false for array datatype and add a warning because
		// datastream does not support array datatypes.
		if ty.IsArray {
			issues = append(issues, internal.ArrayTypeNotSupported)
			isNotNull = false
		}
		// Set auto generation for column
		srcAutoGen := srcCol.AutoGen
		var autoGenCol *ddl.AutoGenCol = &ddl.AutoGenCol{}
		if srcAutoGen.Name != "" {
			autoGenCol, err = toddl.GetColumnAutoGen(conv, srcAutoGen, srcColId, srcTable.Id)
			if autoGenCol != nil {
				if err != nil {
					srcCol.Ignored.AutoIncrement = true
					issues = append(issues, internal.AutoIncrement)
				} else {
					issues = append(issues, internal.SequenceCreated)
				}
			}
		}
		if len(issues) > 0 {
			columnLevelIssues[srcColId] = issues
		}

		spColDef[srcColId] = ddl.ColumnDef{
			Name:    colName,
			T:       ty,
			NotNull: isNotNull,
			Comment: "From: " + quoteIfNeeded(srcCol.Name) + " " + srcCol.Type.Print(),
			Id:      srcColId,
			AutoGen: *autoGenCol,
		}
		if !checkIfColumnIsPartOfPK(srcColId, srcTable.PrimaryKeys) {
			totalNonKeyColumnSize += getColumnSize(ty.Name, ty.Len)
		}
	}
	if totalNonKeyColumnSize > ddl.MaxNonKeyColumnLength {
		tableLevelIssues = append(tableLevelIssues, internal.RowLimitExceeded)
	}
	conv.SchemaIssues[srcTable.Id] = internal.TableIssues{
		TableLevelIssues:  tableLevelIssues,
		ColumnLevelIssues: columnLevelIssues,
	}
	comment := "Spanner schema for source table " + quoteIfNeeded(srcTable.Name)
	conv.SpSchema[srcTable.Id] = ddl.CreateTable{
		Name:             spTableName,
		ColIds:           spColIds,
		ColDefs:          spColDef,
		PrimaryKeys:      cvtPrimaryKeys(srcTable.PrimaryKeys),
		ForeignKeys:      cvtForeignKeys(conv, spTableName, srcTable.Id, srcTable.ForeignKeys, isRestore),
		CheckConstraints: cvtCheckConstraint(conv, srcTable.CheckConstraints),
		Indexes:          cvtIndexes(conv, srcTable.Id, srcTable.Indexes, spColIds, spColDef),
		Comment:          comment,
		Id:               srcTable.Id,
	}
	return nil
}

func (ss *SchemaToSpannerImpl) SchemaToSpannerSequenceHelper(conv *internal.Conv, srcSequence ddl.Sequence) error {
	switch srcSequence.SequenceKind {
	case constants.AUTO_INCREMENT:
		spSequence := ddl.Sequence{
			Name:             srcSequence.Name,
			Id:               srcSequence.Id,
			SequenceKind:     "BIT REVERSED POSITIVE",
			SkipRangeMin:     srcSequence.SkipRangeMin,
			SkipRangeMax:     srcSequence.SkipRangeMax,
			StartWithCounter: srcSequence.StartWithCounter,
		}
		conv.SpSequences[srcSequence.Id] = spSequence
	default:
		spSequence := ddl.Sequence{
			Name:             srcSequence.Name,
			Id:               srcSequence.Id,
			SequenceKind:     "BIT REVERSED POSITIVE",
			SkipRangeMin:     srcSequence.SkipRangeMin,
			SkipRangeMax:     srcSequence.SkipRangeMax,
			StartWithCounter: srcSequence.StartWithCounter,
		}
		conv.SpSequences[srcSequence.Id] = spSequence
	}
	return nil
}

func quoteIfNeeded(s string) string {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsPunct(r) {
			continue
		}
		return strconv.Quote(s)
	}
	return s
}

func cvtPrimaryKeys(srcKeys []schema.Key) []ddl.IndexKey {
	var spKeys []ddl.IndexKey
	for _, k := range srcKeys {
		spKeys = append(spKeys, ddl.IndexKey{ColId: k.ColId, Desc: k.Desc, Order: k.Order})
	}
	return spKeys
}

func cvtForeignKeys(conv *internal.Conv, spTableName string, srcTableId string, srcKeys []schema.ForeignKey, isRestore bool) []ddl.Foreignkey {
	var spKeys []ddl.Foreignkey
	for _, key := range srcKeys {
		spKey, err := CvtForeignKeysHelper(conv, spTableName, srcTableId, key, isRestore)
		if err != nil {
			continue
		}
		spKeys = append(spKeys, spKey)
	}
	return spKeys
}

// cvtCheckConstraint converts check constraints from source to Spanner.
func cvtCheckConstraint(conv *internal.Conv, srcKeys []schema.CheckConstraint) []ddl.CheckConstraint {
	var spcc []ddl.CheckConstraint

	for _, cc := range srcKeys {
		spcc = append(spcc, ddl.CheckConstraint{
			Id:     cc.Id,
			Name:   internal.ToSpannerCheckConstraintName(conv, cc.Name),
			Expr:   cc.Expr,
			ExprId: cc.ExprId,
		})
	}
	return spcc
}

func CvtForeignKeysHelper(conv *internal.Conv, spTableName string, srcTableId string, srcKey schema.ForeignKey, isRestore bool) (ddl.Foreignkey, error) {
	if len(srcKey.ColIds) != len(srcKey.ReferColumnIds) {
		conv.Unexpected(fmt.Sprintf("ConvertForeignKeys: ColIds and referColumns don't have the same lengths: len(columns)=%d, len(referColumns)=%d for source tableId: %s, referenced table: %s", len(srcKey.ColIds), len(srcKey.ReferColumnIds), srcTableId, srcKey.ReferTableId))
		return ddl.Foreignkey{}, fmt.Errorf("ConvertForeignKeys: columns and referColumns don't have the same lengths")
	}

	// check whether spanner refer table exist or not.
	_, isPresent := conv.SpSchema[srcKey.ReferTableId]
	if !isPresent && isRestore {
		return ddl.Foreignkey{}, nil
	}

	// check whether source refer table exist or not.
	_, isPresent = conv.SrcSchema[srcKey.ReferTableId]
	if !isPresent {
		conv.Unexpected(fmt.Sprintf("Can't map foreign key for source tableId: %s, referenced tableId: %s", srcTableId, srcKey.ReferTableId))
		return ddl.Foreignkey{}, fmt.Errorf("reference table not found")
	}
	var spColIds, spReferColIds []string
	for i, colId := range srcKey.ColIds {
		spColIds = append(spColIds, colId)
		spReferColIds = append(spReferColIds, srcKey.ReferColumnIds[i])
	}
	spKeyName := internal.ToSpannerForeignKey(conv, srcKey.Name)
	spDeleteRule := internal.ToSpannerOnDelete(conv, srcTableId, srcKey.OnDelete)
	spUpdateRule := internal.ToSpannerOnUpdate(conv, srcTableId, srcKey.OnUpdate)

	spKey := ddl.Foreignkey{
		Name:           spKeyName,
		ColIds:         spColIds,
		ReferTableId:   srcKey.ReferTableId,
		ReferColumnIds: spReferColIds,
		Id:             srcKey.Id,
		OnDelete:       spDeleteRule,
		OnUpdate:       spUpdateRule,
	}
	return spKey, nil
}

func cvtIndexes(conv *internal.Conv, tableId string, srcIndexes []schema.Index, spColIds []string, spColDef map[string]ddl.ColumnDef) []ddl.CreateIndex {
	var spIndexes []ddl.CreateIndex
	for _, srcIndex := range srcIndexes {
		spIndex := CvtIndexHelper(conv, tableId, srcIndex, spColIds, spColDef)
		if (!reflect.DeepEqual(spIndex, ddl.CreateIndex{})) {
			spIndexes = append(spIndexes, spIndex)
		}
	}
	return spIndexes
}

func SrcTableToSpannerDDL(conv *internal.Conv, toddl ToDdl, srcTable schema.Table, ddlVerifier expressions_api.DDLVerifier) error {
	schemaToSpanner := SchemaToSpannerImpl{
		DdlV: ddlVerifier,
	}
	err := schemaToSpanner.SchemaToSpannerDDLHelper(conv, toddl, srcTable, true)
	if err != nil {
		return err
	}
	for tableId, sourceTable := range conv.SrcSchema {
		if _, isPresent := conv.SpSchema[tableId]; !isPresent {
			continue
		}
		spTable := conv.SpSchema[tableId]
		if tableId != srcTable.Id {
			spTable.ForeignKeys = cvtForeignKeysForAReferenceTable(conv, tableId, srcTable.Id, sourceTable.ForeignKeys, spTable.ForeignKeys)
			conv.SpSchema[tableId] = spTable
		}
	}

	internal.ResolveRefs(conv)
	return nil
}

func cvtForeignKeysForAReferenceTable(conv *internal.Conv, tableId string, referTableId string, srcKeys []schema.ForeignKey, spKeys []ddl.Foreignkey) []ddl.Foreignkey {
	for _, key := range srcKeys {
		if key.ReferTableId == referTableId {
			spKey, err := CvtForeignKeysHelper(conv, conv.SpSchema[tableId].Name, tableId, key, true)
			if err != nil {
				continue
			}

			spKey.Id = key.Id
			spKeys = append(spKeys, spKey)
		}
	}
	return spKeys
}

func CvtIndexHelper(conv *internal.Conv, tableId string, srcIndex schema.Index, spColIds []string, spColDef map[string]ddl.ColumnDef) ddl.CreateIndex {
	var spKeys []ddl.IndexKey
	var spStoredColIds []string

	for _, k := range srcIndex.Keys {
		isPresent := false
		for _, v := range spColIds {
			if v == k.ColId {
				isPresent = true
				if conv.SpDialect == constants.DIALECT_POSTGRESQL {
					if spColDef[v].T.Name == ddl.Numeric {
						// index on NUMERIC is not supported in PGSQL Dialect currently.
						// Indexes which contains a NUMERIC column in it will need to be skipped.
						return ddl.CreateIndex{}
					}
				}
				break
			}
		}
		if !isPresent {
			conv.Unexpected(fmt.Sprintf("Can't map index key column for tableId %s columnId %s", tableId, k.ColId))
			continue
		}
		spKeys = append(spKeys, ddl.IndexKey{ColId: k.ColId, Desc: k.Desc, Order: k.Order})
	}
	for _, colId := range srcIndex.StoredColumnIds {
		isPresent := false
		for _, v := range spColIds {
			if v == colId {
				isPresent = true
				break
			}
		}
		if !isPresent {
			conv.Unexpected(fmt.Sprintf("Can't map index column for tableId %s columnId %s", tableId, colId))
			continue
		}
		spStoredColIds = append(spStoredColIds, colId)
	}
	if srcIndex.Name == "" {
		// Generate a name if index name is empty in MySQL.
		// Collision of index name will be handled by ToSpannerIndexName.
		srcIndex.Name = fmt.Sprintf("Index_%s", conv.SrcSchema[tableId].Name)
	}
	spIndexName := internal.ToSpannerIndexName(conv, srcIndex.Name)
	spIndex := ddl.CreateIndex{
		Name:            spIndexName,
		TableId:         tableId,
		Unique:          srcIndex.Unique,
		Keys:            spKeys,
		StoredColumnIds: spStoredColIds,
		Id:              srcIndex.Id,
	}
	return spIndex
}

// Applies all valid expressions which can be migrated to spanner conv object
func spannerSchemaApplyExpressions(conv *internal.Conv, expressions internal.VerifyExpressionsOutput) {
	for _, expression := range expressions.ExpressionVerificationOutputList {
		switch expression.ExpressionDetail.Type {
		case "DEFAULT":
			{
				tableId := expression.ExpressionDetail.Metadata["TableId"]
				columnId := expression.ExpressionDetail.Metadata["ColId"]

				if expression.Result {
					col := conv.SpSchema[tableId].ColDefs[columnId]
					col.DefaultValue = ddl.DefaultValue{
						IsPresent: true,
						Value: ddl.Expression{
							ExpressionId: expression.ExpressionDetail.ExpressionId,
							Statement:    expression.ExpressionDetail.Expression,
						},
					}
					conv.SpSchema[tableId].ColDefs[columnId] = col
				} else {
					colIssues := conv.SchemaIssues[tableId].ColumnLevelIssues[columnId]
					colIssues = append(colIssues, internal.DefaultValue)
					conv.SchemaIssues[tableId].ColumnLevelIssues[columnId] = colIssues
				}
			}
		}
	}
}
