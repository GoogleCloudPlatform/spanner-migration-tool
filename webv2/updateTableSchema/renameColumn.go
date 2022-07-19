package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

func renameColumn(newName, table, colName, srcTableName string) {

	fmt.Println("renameColumn getting called")

	sessionState := session.GetSessionState()

	sp := sessionState.Conv.SpSchema[table]

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
	renameSpannerSchemaIssue(table, colName, newName)

	// step VIII
	renameToSpannerToSource(table, colName, newName)

	sessionState.Conv.SpSchema[table] = sp

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		fmt.Println("update foreignKey Table column names")

		relationTable := sp.Fks[i].ReferTable

		relationTableSp := sessionState.Conv.SpSchema[relationTable]

		relationTableSp = renameSpannerColNames(relationTableSp, colName, newName)
		relationTableSp = renameSpannerColDefs(relationTableSp, colName, newName)
		relationTableSp = renameSpannerPK(relationTableSp, colName, newName)
		relationTableSp = renameSpannerSecondaryIndex(relationTableSp, colName, newName)
		relationTableSp = renameSpannerForeignkeyColumns(relationTableSp, colName, newName)
		relationTableSp = renameSpannerForeignkeyReferColumns(relationTableSp, colName, newName)

		//todo
		renameToSpannerToSource(relationTable, colName, newName)
		renameSpannerSchemaIssue(relationTable, colName, newName)
		sessionState.Conv.SpSchema[relationTable] = relationTableSp

	}

	// update interleave table relation
	isParent, childSchema := utilities.IsParent(table)

	if isParent {
		fmt.Println("yes", table, "is parent table")

		childSchemaSp := sessionState.Conv.SpSchema[childSchema]

		childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

		//todo
		renameToSpannerToSource(childSchema, colName, newName)
		renameSpannerSchemaIssue(childSchema, colName, newName)

		sessionState.Conv.SpSchema[childSchema] = childSchemaSp

	}

	isChild := sessionState.Conv.SpSchema[table].Parent

	if isChild != "" {

		childSchemaSp := sessionState.Conv.SpSchema[isChild]

		childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

		//todo
		renameToSpannerToSource(isChild, colName, newName)

		renameSpannerSchemaIssue(isChild, colName, newName)

		sessionState.Conv.SpSchema[isChild] = childSchemaSp
	}

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

func renameToSpannerToSource(table string, colName string, newName string) {

	sessionState := session.GetSessionState()
	srcTableName := sessionState.Conv.ToSource[table].Name

	srcColName := sessionState.Conv.ToSource[table].Cols[colName]
	sessionState.Conv.ToSpanner[srcTableName].Cols[srcColName] = newName
	sessionState.Conv.ToSource[table].Cols[newName] = srcColName
	delete(sessionState.Conv.ToSource[table].Cols, colName)

}

func renameSpannerSchemaIssue(table string, colName string, newName string) {

	sessionState := session.GetSessionState()

	if sessionState.Conv.Issues != nil {

		if sessionState.Conv.Issues[table] != nil && sessionState.Conv.Issues[table][colName] != nil {

			schemaissue := sessionState.Conv.Issues[table][colName]

			s := map[string][]internal.SchemaIssue{
				newName: schemaissue,
			}

			sessionState.Conv.Issues[table] = s

		}

	}

	delete(sessionState.Conv.Issues[table], colName)

}
