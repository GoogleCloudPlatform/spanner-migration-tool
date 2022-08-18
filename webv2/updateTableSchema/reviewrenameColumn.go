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

	fmt.Println("columnId :", columnId)

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		reviewRenameForeignkeyTableSchema(Conv, sp, i, colName, newName)

	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				fmt.Println("found")
				fmt.Println("sp.Name :", sp.Name)

				reviewRenameForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newName)
			}

		}

	}

	// update interleave table relation
	isParent, parentSchemaTable := IsParent(table)

	fmt.Println("parentSchemaTable :", parentSchemaTable)

	if isParent {

		reviewRenameparentTableSchema(Conv, parentSchemaTable, interleaveTableSchema, colName, newName)
	}

	//10
	childSchemaTable := Conv.SpSchema[table].Parent

	fmt.Println("childSchemaTable :", childSchemaTable)

	if childSchemaTable != "" {

		reviewRenamechildTableSchema(Conv, childSchemaTable, interleaveTableSchema, colName, newName)

	}

	interleaveTableSchema = reanmeCurrentTable(Conv, sp, interleaveTableSchema, table, columnId, colName, newName, parentSchemaTable, childSchemaTable)

	return interleaveTableSchema

}

func reviewRenameForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newName string) {

	fmt.Println("update foreignKey Table column names")
	fmt.Println("")

	relationTable := sp.Fks[index].ReferTable

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

func reviewRenameForeignkeyReferTableSchema(Conv *internal.Conv, referTable ddl.CreateTable, table string, colName string, newName string) {

	// step I
	referTable = renameSpannerColDefs(referTable, colName, newName)

	// step II
	referTable = renameSpannerPK(referTable, colName, newName)

	// step III
	referTable = renameSpannerSecondaryIndex(referTable, colName, newName)

	// step IV
	referTable = renameSpannerForeignkeyColumns(referTable, colName, newName)

	// step V
	referTable = renameSpannerForeignkeyReferColumns(referTable, colName, newName)

	// step VI
	referTable = renameSpannerColNames(referTable, colName, newName)

	// step VII
	renameSpannerSchemaIssue(table, colName, newName, Conv)

	// step VIII
	renameToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = referTable

}

func reviewRenameparentTableSchema(Conv *internal.Conv, parentSchemaTable string, interleaveTableSchema []InterleaveTableSchema, colName string, newName string) []InterleaveTableSchema {

	childSchemaSp := Conv.SpSchema[parentSchemaTable]

	columnId := childSchemaSp.ColDefs[colName].Id

	fmt.Println("columnId :", columnId)

	_, ok := childSchemaSp.ColDefs[colName]

	if ok {
		{
			childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

			renameToSpannerToSource(parentSchemaTable, colName, newName, Conv)
			renameSpannerSchemaIssue(parentSchemaTable, colName, newName, Conv)

			Conv.SpSchema[parentSchemaTable] = childSchemaSp

			fmt.Println("childSchema :", parentSchemaTable)

			interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, parentSchemaTable, columnId, colName, newName)

		}
	}

	return interleaveTableSchema
}

func reviewRenamechildTableSchema(Conv *internal.Conv, childSchemaTable string, interleaveTableSchema []InterleaveTableSchema, colName string, newName string) {

	childSchemaSp := Conv.SpSchema[childSchemaTable]

	_, ok := childSchemaSp.ColDefs[colName]

	columnId := childSchemaSp.ColDefs[colName].Id

	fmt.Println("columnId :", columnId)

	if ok {
		{

			childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
			childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

			//todo
			renameToSpannerToSource(childSchemaTable, colName, newName, Conv)

			renameSpannerSchemaIssue(childSchemaTable, colName, newName, Conv)

			//11
			Conv.SpSchema[childSchemaTable] = childSchemaSp

			{
				fmt.Println("childSchemaTable :", childSchemaTable)

				interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, childSchemaTable, columnId, colName, newName)
			}

		}
	}
}

func renameSpannerColNames(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step I
	// update sp.ColNames
	//fmt.Println("")
	//	fmt.Println("step I")

	for i, col := range sp.ColNames {
		if col == colName {
			//	fmt.Println("renaming sp.ColNames : ")
			sp.ColNames[i] = newName
			//	fmt.Println("renamed sp.ColNames[i] : ", sp.ColNames[i])
			break
		}
	}

	return sp

}

func renameSpannerColDefs(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step II
	// update sp.ColDefs
	//fmt.Println("")
	//fmt.Println("step II")

	if _, found := sp.ColDefs[colName]; found {
		//	fmt.Println("renaming sp.ColDefs : ")

		sp.ColDefs[newName] = ddl.ColumnDef{
			Name:    newName,
			T:       sp.ColDefs[colName].T,
			NotNull: sp.ColDefs[colName].NotNull,
			Comment: sp.ColDefs[colName].Comment,
			Id:      sp.ColDefs[colName].Id,
		}

		//	fmt.Println("renamed sp.ColDefs[newName]", sp.ColDefs[newName].Name)

		delete(sp.ColDefs, colName)
	}

	return sp
}

func renameSpannerPK(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step III
	// update sp.Pks
	//	fmt.Println("")
	//	fmt.Println("step III")

	for i, pk := range sp.Pks {
		if pk.Col == colName {

			//	fmt.Println("renaming sp.Pks : ")

			sp.Pks[i].Col = newName

			//	fmt.Println("renamed sp.Pks[i].Col : ", sp.Pks[i].Col)

			break
		}
	}

	return sp
}

func renameSpannerSecondaryIndex(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step IV
	// update sp.Indexes
	//	fmt.Println("")
	//	fmt.Println("step IV")

	for i, index := range sp.Indexes {
		for j, key := range index.Keys {
			if key.Col == colName {

				//		fmt.Println("renaming sp.Indexes[i].Keys[j].Col : ")

				sp.Indexes[i].Keys[j].Col = newName

				//		fmt.Println("renamed sp.Indexes[i].Keys[j].Col : ", sp.Indexes[i].Keys[j].Col)

				break
			}
		}
	}

	return sp
}

func renameSpannerForeignkeyColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step V
	// update sp.Fks
	//	fmt.Println("")
	//	fmt.Println("step V")

	for i, fk := range sp.Fks {
		for j, column := range fk.Columns {
			if column == colName {

				//	fmt.Println("renaming sp.Fks[i].Columns[j] :")

				sp.Fks[i].Columns[j] = newName

				//	fmt.Println("renamed sp.Fks[i].Columns[j] :", sp.Fks[i].Columns[j])

			}
		}
	}

	return sp
}

func renameSpannerForeignkeyReferColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	//fmt.Println("")
	//	fmt.Println("step VI")

	// step VI
	// update sp.Fks.ReferColumns

	for i, fk := range sp.Fks {
		for j, column := range fk.ReferColumns {

			if column == colName {

				//		fmt.Println("renaming sp.Fks[i].ReferColumns[j] :", sp.Fks[i].ReferColumns[j])
				sp.Fks[i].ReferColumns[j] = newName

				//		fmt.Println("renamed sp.Fks[i].ReferColumns[j] :", sp.Fks[i].Columns[j])

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

func reanmeCurrentTable(Conv *internal.Conv, sp ddl.CreateTable, interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, newName string, childSchemaTable string, parentSchemaTable string) []InterleaveTableSchema {
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

	if parentSchemaTable != "" || childSchemaTable != "" {
		interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, table, columnId, colName, newName)

	}

	return interleaveTableSchema
}
