package webv2

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func renameColumn(newName, table, colName, srcTableName string) {

	fmt.Println("renameColumn getting called")

	sessionState := session.GetSessionState()

	sp := sessionState.Conv.SpSchema[table]

	// step I
	// update sp.ColNames
	fmt.Println("")
	fmt.Println("step I")

	sp = convColNames(sp, colName, newName)

	// step II
	// update sp.ColDefs
	fmt.Println("")
	fmt.Println("step II")

	sp = renameColDefs(sp, colName, newName)

	// step III
	// update sp.Pks
	fmt.Println("")
	fmt.Println("step III")
	sp = renamePK(sp, colName, newName)

	// step IV
	// update sp.Indexes
	fmt.Println("")
	fmt.Println("step IV")

	sp = renameIndex(sp, colName, newName)

	// step V
	// update sp.Fks
	fmt.Println("")
	fmt.Println("step V")

	sp = renameForeignkey(sp, colName, newName)

	fmt.Println("")
	fmt.Println("step VI")

	// step VI
	// update sp.Fks.ReferColumns

	sp = renameReferColumns(sp, colName, newName)

	srcColName := sessionState.Conv.ToSource[table].Cols[colName]
	sessionState.Conv.ToSpanner[srcTableName].Cols[srcColName] = newName
	sessionState.Conv.ToSource[table].Cols[newName] = srcColName
	delete(sessionState.Conv.ToSource[table].Cols, colName)

	sessionState.Conv.SpSchema[table] = sp

	// update foreignKey Table column names
	for i, _ := range sp.Fks {

		relationTable := sp.Fks[i].ReferTable

		relationTableSp := sessionState.Conv.SpSchema[relationTable]

		relationTableSp = convColNames(relationTableSp, colName, newName)
		relationTableSp = renameColDefs(relationTableSp, colName, newName)
		relationTableSp = renamePK(relationTableSp, colName, newName)
		relationTableSp = renameIndex(relationTableSp, colName, newName)
		relationTableSp = renameForeignkey(relationTableSp, colName, newName)
		relationTableSp = renameReferColumns(relationTableSp, colName, newName)

		//todo
		renameToSpannerToSource(relationTable, colName, newName)

		sessionState.Conv.SpSchema[relationTable] = relationTableSp

	}

	// update interleave table relation
	isParent, childSchema := isParent(table)

	if isParent {
		fmt.Println("yes", table, "is parent table")

		childSchemaSp := sessionState.Conv.SpSchema[childSchema]

		childSchemaSp = convColNames(childSchemaSp, colName, newName)
		childSchemaSp = renameColDefs(childSchemaSp, colName, newName)
		childSchemaSp = renamePK(childSchemaSp, colName, newName)
		childSchemaSp = renameIndex(childSchemaSp, colName, newName)
		childSchemaSp = renameForeignkey(childSchemaSp, colName, newName)
		childSchemaSp = renameReferColumns(childSchemaSp, colName, newName)

		//todo
		renameToSpannerToSource(childSchema, colName, newName)

		sessionState.Conv.SpSchema[childSchema] = childSchemaSp

	}

	isChild := sessionState.Conv.SpSchema[table].Parent

	if isChild != "" {

		childSchemaSp := sessionState.Conv.SpSchema[isChild]

		childSchemaSp = convColNames(childSchemaSp, colName, newName)
		childSchemaSp = renameColDefs(childSchemaSp, colName, newName)
		childSchemaSp = renamePK(childSchemaSp, colName, newName)
		childSchemaSp = renameIndex(childSchemaSp, colName, newName)
		childSchemaSp = renameForeignkey(childSchemaSp, colName, newName)
		childSchemaSp = renameReferColumns(childSchemaSp, colName, newName)

		//todo
		renameToSpannerToSource(isChild, colName, newName)

		sessionState.Conv.SpSchema[isChild] = childSchemaSp
	}

	fmt.Printf("column : '%s' in table : '%s' is part of parent-child relation with schema : '%s'", colName, table, childSchema)
	fmt.Println("isChild :", isChild)

}

func convColNames(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

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

func renameColDefs(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

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

func renamePK(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

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

func renameIndex(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

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

func renameForeignkey(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

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

func renameReferColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {
	for i, fk := range sp.Fks {
		for j, column := range fk.ReferColumns {

			if column == colName {

				fmt.Println("sp.Fks[i].ReferColumns[j] :")
				sp.Fks[i].ReferColumns[j] = newName

				fmt.Println("renamed sp.Fks[i].ReferColumns[j] :", sp.Fks[i].Columns[j])

			}

		}
	}
	return sp
}

func renameToSpannerToSource(table string, colName string, newName string) {

	sessionState := session.GetSessionState()
	srcTableName := sessionState.Conv.ToSource[table].Name

	srcColName := sessionState.Conv.ToSource[table].Cols[colName]
	sessionState.Conv.ToSpanner[srcTableName].Cols[srcColName] = newName
	sessionState.Conv.ToSource[table].Cols[newName] = srcColName
	delete(sessionState.Conv.ToSource[table].Cols, colName)

}
