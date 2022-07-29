package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func reviewRenameColumn(newName, table, colName string, Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema) []InterleaveTableSchema {

	fmt.Println("renameColumn getting called")
	fmt.Println("")

	sp := Conv.SpSchema[table]

	columnId := sp.ColDefs[colName].Id

	// step I
	sp = renameSpannerColDefs(sp, colName, newName)

	// step II
	sp = renameSpannerPK(sp, colName, newName)

	// step III
	sp = renameSpannerSecondaryIndex(sp, colName, newName)

	// step IV
	sp = renameSpannerForeignkeyColumns(sp, colName, newName)

	// step V
	sp = renameSpannerForeignkeyReferColumns(sp, colName, newName)

	// step VI
	sp = renameSpannerColNames(sp, colName, newName)

	// step VII
	renameSpannerSchemaIssue(table, colName, newName, Conv)

	// step VIII
	renameToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = sp

	{

		interleaveTableSchema = updatenameinterleaveTableSchema(interleaveTableSchema, table, columnId, colName, newName)

	}

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		fmt.Println("update foreignKey Table column names")
		fmt.Println("")

		relationTable := sp.Fks[i].ReferTable

		relationTableSp := Conv.SpSchema[relationTable]

		_, ok := relationTableSp.ColDefs[colName]

		if ok {
			{
				relationTableSp = renameSpannerColNames(relationTableSp, colName, newName)
				relationTableSp = renameSpannerColDefs(relationTableSp, colName, newName)
				relationTableSp = renameSpannerPK(relationTableSp, colName, newName)
				relationTableSp = renameSpannerSecondaryIndex(relationTableSp, colName, newName)
				relationTableSp = renameSpannerForeignkeyColumns(relationTableSp, colName, newName)
				relationTableSp = renameSpannerForeignkeyReferColumns(relationTableSp, colName, newName)

				renameToSpannerToSource(relationTable, colName, newName, Conv)
				renameSpannerSchemaIssue(relationTable, colName, newName, Conv)

				Conv.SpSchema[relationTable] = relationTableSp

			}
		}

	}

	// update interleave table relation
	isParent, childSchema := IsParent(table)

	if isParent {
		fmt.Println("yes", table, "is parent table")

		childSchemaSp := Conv.SpSchema[childSchema]

		columnId := childSchemaSp.ColDefs[colName].Id

		_, ok := childSchemaSp.ColDefs[colName]

		if ok {
			{
				childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

				renameToSpannerToSource(childSchema, colName, newName, Conv)
				renameSpannerSchemaIssue(childSchema, colName, newName, Conv)

				Conv.SpSchema[childSchema] = childSchemaSp

				{

					interleaveTableSchema = updatenameinterleaveTableSchema(interleaveTableSchema, childSchema, columnId, colName, newName)

				}

			}
		}
	}

	//10
	isChild := Conv.SpSchema[table].Parent

	if isChild != "" {

		childSchemaSp := Conv.SpSchema[isChild]

		_, ok := childSchemaSp.ColDefs[colName]

		columnId := childSchemaSp.ColDefs[colName].Id

		if ok {
			{

				childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
				childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

				//todo
				renameToSpannerToSource(isChild, colName, newName, Conv)

				renameSpannerSchemaIssue(isChild, colName, newName, Conv)

				//11
				Conv.SpSchema[isChild] = childSchemaSp

				{

					interleaveTableSchema = updatenameinterleaveTableSchema(interleaveTableSchema, childSchema, columnId, colName, newName)

				}

			}
		}

	}

	return interleaveTableSchema

}

func updatenameinterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, newName string) []InterleaveTableSchema {

	index := isTablePresent(interleaveTableSchema, table)

	if index == -1 {
		itc := InterleaveTableSchema{}

		itc.Table = table
		itc.InterleaveColumnChanges = []InterleaveColumn{}

		{
			ic := InterleaveColumn{}

			ic.ColumnId = columnId

			fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$")
			fmt.Println("updatenameinterleaveTableSchema  ic.ColumnId :", ic.ColumnId)
			fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$")

			ic.ColumnName = colName
			ic.UpdateColumnName = newName

			fmt.Println("I am trying to append :")
			fmt.Println("InterleaveColumn", ic)

			itc.InterleaveColumnChanges = append(itc.InterleaveColumnChanges, ic)

			interleaveTableSchema = append(interleaveTableSchema, itc)
		}

		return interleaveTableSchema
	}

	interleaveTableSchema[index].InterleaveColumnChanges = getInterleaveColumn(interleaveTableSchema[index].InterleaveColumnChanges, columnId, colName, newName)

	return interleaveTableSchema
}

func isTablePresent(interleaveTableSchema []InterleaveTableSchema, table string) int {

	for i := 0; i < len(interleaveTableSchema); i++ {

		if interleaveTableSchema[i].Table == table {
			return i
		}

	}

	return -1
}

func getInterleaveColumn(interleaveColumn []InterleaveColumn, columnId string, colName string, newName string) []InterleaveColumn {

	index := isColumnPresent(interleaveColumn, columnId)

	if index == -1 {

		ic := InterleaveColumn{}
		ic.ColumnId = columnId
		ic.ColumnName = colName
		ic.UpdateColumnName = newName
		interleaveColumn = append(interleaveColumn, ic)

		return interleaveColumn
	}

	interleaveColumn[index].ColumnName = colName
	interleaveColumn[index].UpdateColumnName = newName

	return interleaveColumn
}

func isColumnPresent(interleaveColumn []InterleaveColumn, columnId string) int {

	for i := 0; i < len(interleaveColumn); i++ {

		if interleaveColumn[i].ColumnId == columnId {
			return i
		}

	}

	return -1
}

func renameSpannerColNames(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step I
	// update sp.ColNames
	fmt.Println("")
	fmt.Println("step I")

	for i, col := range sp.ColNames {
		if col == colName {
			fmt.Println("renaming sp.ColNames : ")
			sp.ColNames[i] = newName
			fmt.Println("renamed sp.ColNames[i] : ", sp.ColNames[i])
			break
		}
	}

	return sp

}

func renameSpannerColDefs(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step II
	// update sp.ColDefs
	fmt.Println("")
	fmt.Println("step II")

	if _, found := sp.ColDefs[colName]; found {
		fmt.Println("renaming sp.ColDefs : ")

		sp.ColDefs[newName] = ddl.ColumnDef{
			Name:    newName,
			T:       sp.ColDefs[colName].T,
			NotNull: sp.ColDefs[colName].NotNull,
			Comment: sp.ColDefs[colName].Comment,
			Id:      sp.ColDefs[colName].Id,
		}

		fmt.Println("renamed sp.ColDefs[newName]", sp.ColDefs[newName].Name)

		delete(sp.ColDefs, colName)
	}

	return sp
}

func renameSpannerPK(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step III
	// update sp.Pks
	fmt.Println("")
	fmt.Println("step III")

	for i, pk := range sp.Pks {
		if pk.Col == colName {

			fmt.Println("renaming sp.Pks : ")

			sp.Pks[i].Col = newName

			fmt.Println("renamed sp.Pks[i].Col : ", sp.Pks[i].Col)

			break
		}
	}

	return sp
}

func renameSpannerSecondaryIndex(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step IV
	// update sp.Indexes
	fmt.Println("")
	fmt.Println("step IV")

	for i, index := range sp.Indexes {
		for j, key := range index.Keys {
			if key.Col == colName {

				fmt.Println("renaming sp.Indexes[i].Keys[j].Col : ")

				sp.Indexes[i].Keys[j].Col = newName

				fmt.Println("renamed sp.Indexes[i].Keys[j].Col : ", sp.Indexes[i].Keys[j].Col)

				break
			}
		}
	}

	return sp
}

func renameSpannerForeignkeyColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step V
	// update sp.Fks
	fmt.Println("")
	fmt.Println("step V")

	for i, fk := range sp.Fks {
		for j, column := range fk.Columns {
			if column == colName {

				fmt.Println("renaming sp.Fks[i].Columns[j] :")

				sp.Fks[i].Columns[j] = newName

				fmt.Println("renamed sp.Fks[i].Columns[j] :", sp.Fks[i].Columns[j])

			}
		}
	}

	return sp
}

func renameSpannerForeignkeyReferColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	fmt.Println("")
	fmt.Println("step VI")

	// step VI
	// update sp.Fks.ReferColumns

	for i, fk := range sp.Fks {
		for j, column := range fk.ReferColumns {

			if column == colName {

				fmt.Println("renaming sp.Fks[i].ReferColumns[j] :", sp.Fks[i].ReferColumns[j])
				sp.Fks[i].ReferColumns[j] = newName

				fmt.Println("renamed sp.Fks[i].ReferColumns[j] :", sp.Fks[i].Columns[j])

			}

		}
	}
	return sp
}

func renameToSpannerToSource(table string, colName string, newName string, Conv *internal.Conv) {

	srcTableName := Conv.ToSource[table].Name

	srcColName := Conv.ToSource[table].Cols[colName]

	Conv.ToSpanner[srcTableName].Cols[srcColName] = newName
	Conv.ToSource[table].Cols[newName] = srcColName
	delete(Conv.ToSource[table].Cols, colName)

}

func renameSpannerSchemaIssue(table string, colName string, newName string, Conv *internal.Conv) {

	//12
	if Conv.Issues != nil {

		if Conv.Issues[table] != nil && Conv.Issues[table][colName] != nil {

			schemaissue := Conv.Issues[table][colName]

			s := map[string][]internal.SchemaIssue{
				newName: schemaissue,
			}

			Conv.Issues[table] = s

		}

	}

	delete(Conv.Issues[table], colName)

}
