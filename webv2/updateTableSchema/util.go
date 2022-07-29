package updateTableSchema

func updatetypeinterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, colName string, columnId string, previoustype string, updateType string) []InterleaveTableSchema {

	index := isTablePresent(interleaveTableSchema, table)

	if index == -1 {
		itc := InterleaveTableSchema{}

		itc.Table = table
		itc.InterleaveColumnChanges = []InterleaveColumn{}

		ic := InterleaveColumn{}
		ic.ColumnId = columnId
		ic.ColumnName = colName
		ic.Type = previoustype
		ic.UpdateType = updateType

		itc.InterleaveColumnChanges = append(itc.InterleaveColumnChanges, ic)

		interleaveTableSchema = append(interleaveTableSchema, itc)

		return interleaveTableSchema
	}

	interleaveTableSchema[index].InterleaveColumnChanges = getInterleaveColumnfortype(interleaveTableSchema[index].InterleaveColumnChanges, columnId, colName, previoustype, updateType)

	return interleaveTableSchema
}

func getInterleaveColumnfortype(interleaveColumn []InterleaveColumn, columnId string, colName string, previoustype string, updateType string) []InterleaveColumn {

	index := isColumnPresent(interleaveColumn, columnId)

	if index == -1 {

		ic := InterleaveColumn{}
		ic.ColumnId = columnId
		ic.ColumnName = colName
		ic.Type = previoustype
		ic.UpdateType = updateType
		interleaveColumn = append(interleaveColumn, ic)

		return interleaveColumn
	}

	interleaveColumn[index].ColumnName = colName
	interleaveColumn[index].Type = previoustype
	interleaveColumn[index].UpdateType = updateType

	return interleaveColumn
}
