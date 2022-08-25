package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// IsColumnPresentInColNames check column is present in colnames.
func IsColumnPresentInColNames(colNames []string, columnName string) bool {

	for _, column := range colNames {
		if column == columnName {
			return true
		}
	}

	return false
}

// GetSpannerTableDDL return Spanner Table DDL as string.
func GetSpannerTableDDL(spannerTable ddl.CreateTable) string {

	c := ddl.Config{Comments: true, ProtectIds: false}

	ddl := spannerTable.PrintCreateTable(c)

	return ddl
}

func renameinterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, newName string) []InterleaveTableSchema {

	tindex := isTablePresent(interleaveTableSchema, table)

	interleaveTableSchema = createinterleaveTableSchema(interleaveTableSchema, table, tindex)

	fmt.Println("interleaveTableSchema :", interleaveTableSchema)

	interleaveTableSchema = renameInterleaveColumn(interleaveTableSchema, table, columnId, colName, newName)

	return interleaveTableSchema
}

func isTablePresent(interleaveTableSchema []InterleaveTableSchema, table string) int {

	fmt.Println("isTablePresent getting called")

	for i := 0; i < len(interleaveTableSchema); i++ {

		if interleaveTableSchema[i].Table == table {

			fmt.Println("table :", table)
			return i
		}

	}

	return -1
}

func createinterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, tindex int) []InterleaveTableSchema {

	if tindex == -1 {
		itc := InterleaveTableSchema{}
		itc.Table = table
		itc.InterleaveColumnChanges = []InterleaveColumn{}

		interleaveTableSchema = append(interleaveTableSchema, itc)
	}

	return interleaveTableSchema
}

func renameInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, table, columnId, colName, newName string) []InterleaveTableSchema {

	tindex := isTablePresent(interleaveTableSchema, table)

	fmt.Println("tindex :", tindex)

	cindex := isColumnPresent(interleaveTableSchema[tindex].InterleaveColumnChanges, columnId)

	fmt.Println("cindex :", cindex)

	interleaveTableSchema = createInterleaveColumn(interleaveTableSchema, tindex, cindex, columnId, colName, newName)

	if tindex != -1 && cindex != -1 {
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].ColumnId = columnId
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].ColumnName = colName
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].UpdateColumnName = newName

	}

	return interleaveTableSchema

}

func createInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, tindex int, cindex int, columnId string, colName string, newName string) []InterleaveTableSchema {

	if cindex == -1 {

		if columnId != "" {

			ic := InterleaveColumn{}
			ic.ColumnId = columnId
			ic.ColumnName = colName
			ic.UpdateColumnName = newName

			interleaveTableSchema[tindex].InterleaveColumnChanges = append(interleaveTableSchema[tindex].InterleaveColumnChanges, ic)

		}
	}

	return interleaveTableSchema
}

func isColumnPresent(interleaveColumn []InterleaveColumn, columnId string) int {

	fmt.Println("isColumnPresent getting called ")
	fmt.Println("")

	for i := 0; i < len(interleaveColumn); i++ {

		if interleaveColumn[i].ColumnId == columnId {
			return i
		}

	}

	return -1
}

func typeinterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, previoustype string, updateType string) []InterleaveTableSchema {

	tindex := isTablePresent(interleaveTableSchema, table)

	interleaveTableSchema = createinterleaveTableSchema(interleaveTableSchema, table, tindex)

	fmt.Println("interleaveTableSchema :", interleaveTableSchema)

	interleaveTableSchema = typeInterleaveColumn(interleaveTableSchema, table, columnId, colName, previoustype, updateType)
	return interleaveTableSchema
}

func typeInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, table, columnId, colName, previoustype, updateType string) []InterleaveTableSchema {

	tindex := isTablePresent(interleaveTableSchema, table)

	fmt.Println("tindex :", tindex)

	cindex := isColumnPresent(interleaveTableSchema[tindex].InterleaveColumnChanges, columnId)

	fmt.Println("cindex :", cindex)

	interleaveTableSchema = createInterleaveColumntype(interleaveTableSchema, tindex, cindex, columnId, colName, previoustype, updateType)

	if tindex != -1 && cindex != -1 {
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].ColumnId = columnId
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].ColumnName = colName
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].Type = previoustype
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].UpdateType = updateType

	}

	return interleaveTableSchema

}

func createInterleaveColumntype(interleaveTableSchema []InterleaveTableSchema, tindex int, cindex int, columnId string, colName string, previoustype string, updateType string) []InterleaveTableSchema {

	if cindex == -1 {

		if columnId != "" {

			ic := InterleaveColumn{}
			ic.ColumnId = columnId
			ic.ColumnName = colName
			ic.Type = previoustype
			ic.UpdateType = updateType
			interleaveTableSchema[tindex].InterleaveColumnChanges = append(interleaveTableSchema[tindex].InterleaveColumnChanges, ic)

		}
	}

	return interleaveTableSchema
}
