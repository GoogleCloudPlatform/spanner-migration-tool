package utils

import (
	"fmt"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"golang.org/x/exp/rand"
)

const TablePerDbError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Cannot add table table_999: too many tables (limit 5000)."
const ColumnPerTableError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Table LargeTable has too many columns; the limit is 1024."
const InterleaveDepthError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Table level8 is too deeply nested; the limit is 8 tables."
const ColumnKeyPerTableError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = Table cart_extended has too many keys (17); the limit is 16."
const TableNameError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = table name not valid: CustomerOrderTransactionHistoryRecords2023ForAnalysisAndArchivingIncludingSensitiveDataAndSecureProcessingProceduressrdfgdnhydbtsfvfs."
const ColumnNameError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = Column name not valid: large_column.CustomerOrderTransactionHistoryRecords2023ForAnalysisAndArchivingIncludingSensitiveDataAndSecureProcessingProceduressrdfgdnhydbtsfvfs."

const TablePerDbExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Cannot add table table_999: too many tables (limit 5000)."
const ColumnPerTableExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Table LargeTable has too many columns; the limit is 1024."
const InterleaveDepthExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Table level8 is too deeply nested; the limit is 8 tables."
const ColumnKeyPerTableExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = Table cart_extended has too many keys (17); the limit is 16."
const TableNameExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = table name not valid: CustomerOrderTransactionHistoryRecords2023ForAnalysisAndArchivingIncludingSensitiveDataAndSecureProcessingProceduressrdfgdnhydbtsfvfs."
const ColumnNameExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = Column name not valid: large_column.CustomerOrderTransactionHistoryRecords2023ForAnalysisAndArchivingIncludingSensitiveDataAndSecureProcessingProceduressrdfgdnhydbtsfvfs."

// GenerateRandomString generates a random string of a specified length.
func GenerateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var seededRand = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	randomString := make([]byte, length)
	for i := range randomString {
		randomString[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(randomString)
}

// GenerateColumnDefsForTable generates an array of column definitions for a table
// based on the specified length.
func GenerateColumnDefsForTable(count int) map[string]ddl.ColumnDef {
	colums := make(map[string]ddl.ColumnDef)
	for i := 1; i <= count; i++ {
		colName := fmt.Sprintf("col%d", i)
		colId := fmt.Sprintf("c%d", i)
		colums[colId] = ddl.ColumnDef{Name: colName, Id: colId, T: ddl.Type{Name: ddl.Int64}}
	}
	return colums
}

// GenerateColIds generates an array of column ids for a table
// based on the specified length.
func GenerateColIds(count int) []string {
	var colIds []string
	for i := 1; i <= count; i++ {
		colId := fmt.Sprintf("c%d", i)
		colIds = append(colIds, colId)
	}
	return colIds
}

// GeneratePrimaryColIds generates an array of primary columns ids for a table
// based on the specified length.
func GeneratePrimaryColIds(count int) []ddl.IndexKey {
	var primaryKeys []ddl.IndexKey
	for i := 1; i <= count; i++ {
		colId := fmt.Sprintf("c%d", i)
		primaryKeys = append(primaryKeys, ddl.IndexKey{ColId: colId})
	}
	return primaryKeys
}

// GenerateSpSchema generates a schema consisting of a specified number of tables.
// Each table in the schema is defined by unique properties including identifiers,
// primary keys, columns, and foreign keys, which are set based on the
// iteration index and relationships with other tables.
func GenerateSpSchema(count int) map[string]ddl.CreateTable {
	spschema := make(map[string]ddl.CreateTable)
	for i := 1; i <= count; i++ {
		tableId := fmt.Sprintf("t%d", i)
		tableName := fmt.Sprintf("table%d", i)
		referTableId := fmt.Sprintf("t%d", i-1)
		spschema[tableId] = ddl.CreateTable{
			Name:        "table1",
			Id:          tableName,
			PrimaryKeys: GeneratePrimaryColIds(i),
			ColIds:      GenerateColIds(i + 1),
			ColDefs:     GenerateColumnDefsForTable(i + 1),
			ForeignKeys: GenerateForeignKeys(i-1, referTableId),
		}
	}

	return spschema
}

// GenerateForeignKeys generates an array of foreign keys for a table
// based on the specified length.
func GenerateForeignKeys(count int, referTableId string) []ddl.Foreignkey {
	if count != 0 {
		var colIds []string
		var referColumnIds []string
		for i := 1; i <= count; i++ {
			colId := fmt.Sprintf("c%d", i)
			colIds = append(colIds, colId)
			referColumnIds = append(referColumnIds, colId)
		}
		fname := fmt.Sprintf("level%d_ibfk_1", count)
		return []ddl.Foreignkey{{
			Name:           fname,
			ColIds:         colIds,
			ReferColumnIds: referColumnIds,
			ReferTableId:   referTableId,
			Id:             GenerateRandomString(2),
			OnDelete:       "NO ACTION",
			OnUpdate:       "NO ACTION",
		}}
	} else {
		return nil
	}

}

// GenerateTables generates an array of tables
// based on the specified length.
func GenerateTables(count int) ddl.Schema {
	tables := make(ddl.Schema)

	for i := 1; i <= count; i++ {
		tableName := fmt.Sprintf("table%d", i)
		tableId := fmt.Sprintf("t%d", i)
		tables[tableId] = ddl.CreateTable{Name: tableName, Id: tableId, PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}}, ColIds: []string{"c1"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1": {Name: "col1", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			}}
	}
	return tables
}
