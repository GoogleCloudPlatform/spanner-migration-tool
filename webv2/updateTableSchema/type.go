package updateTableSchema

import "fmt"

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
