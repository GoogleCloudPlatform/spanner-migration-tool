package updateTableSchema

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// renameColumn renames given column to newname and update in schema.
func renameColumn(newName, table, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]

	renameColumnNameInCurrentTableSchema(Conv, sp, table, colName, newName)

	// update foreignKey relationship Table
	for i, _ := range sp.Fks {

		renameColumnNameInForeignkeyTableSchema(Conv, sp, i, colName, newName)
	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				renameColumnNameInForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newName)
			}

		}

	}

	// update interleave table relation
	isParent, parentschemaTable := IsParent(table)

	if isParent {
		renameColumnNameInParentTableSchema(Conv, parentschemaTable, colName, newName)

	}

	childschemaTable := Conv.SpSchema[table].Parent

	if childschemaTable != "" {

		renameColumnNameInchildTableSchema(Conv, childschemaTable, colName, newName)

	}

}

// renameColumnNameInCurrentTableSchema renames given column in Current Table Schema.
func renameColumnNameInCurrentTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string, newName string) {
	// step I
	sp = renameColumnNameInSpannerColDefs(sp, colName, newName)

	// step II
	sp = renameColumnNameInSpannerPK(sp, colName, newName)

	// step III
	sp = renameColumnNameInSpannerSecondaryIndex(sp, colName, newName)

	// step IV
	sp = renameColumnNameInSpannerForeignkeyColumns(sp, colName, newName)

	// step V
	sp = renameColumnNameInSpannerForeignkeyReferColumns(sp, colName, newName)

	// step VI
	sp = renameColumnNameInSpannerColNames(sp, colName, newName)

	// step VII
	renameColumnNameInSpannerSchemaIssue(table, colName, newName, Conv)

	// step VIII
	renameColumnNameInToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = sp

}

// renameColumnNameInForeignkeyTableSchema renames given column in Foreignkey Table Schema.
func renameColumnNameInForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newName string) {

	relationTable := sp.Fks[index].ReferTable

	relationTableSp := Conv.SpSchema[relationTable]

	relationTableSp = renameColumnNameInSpannerColNames(relationTableSp, colName, newName)
	relationTableSp = renameColumnNameInSpannerColDefs(relationTableSp, colName, newName)
	relationTableSp = renameColumnNameInSpannerPK(relationTableSp, colName, newName)
	relationTableSp = renameColumnNameInSpannerSecondaryIndex(relationTableSp, colName, newName)
	relationTableSp = renameColumnNameInSpannerForeignkeyColumns(relationTableSp, colName, newName)
	relationTableSp = renameColumnNameInSpannerForeignkeyReferColumns(relationTableSp, colName, newName)

	renameColumnNameInToSpannerToSource(relationTable, colName, newName, Conv)
	renameColumnNameInSpannerSchemaIssue(relationTable, colName, newName, Conv)

	Conv.SpSchema[relationTable] = relationTableSp

}

// renameColumnNameInForeignkeyReferTableSchema renames given column in Foreignkey Refer Table Schema.
func renameColumnNameInForeignkeyReferTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string, newName string) {
	// step I
	sp = renameColumnNameInSpannerColDefs(sp, colName, newName)

	// step II
	sp = renameColumnNameInSpannerPK(sp, colName, newName)

	// step III
	sp = renameColumnNameInSpannerSecondaryIndex(sp, colName, newName)

	// step IV
	sp = renameColumnNameInSpannerForeignkeyColumns(sp, colName, newName)

	// step V
	sp = renameColumnNameInSpannerForeignkeyReferColumns(sp, colName, newName)

	// step VI
	sp = renameColumnNameInSpannerColNames(sp, colName, newName)

	// step VII
	renameColumnNameInSpannerSchemaIssue(table, colName, newName, Conv)

	// step VIII
	renameColumnNameInToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = sp
}

// renameColumnNameInparentTableSchema renames given column in Parent Table Schema.
func renameColumnNameInParentTableSchema(Conv *internal.Conv, parentschemaTable string, colName string, newName string) {

	parentSchemaSp := Conv.SpSchema[parentschemaTable]

	parentSchemaSp = renameColumnNameInSpannerColNames(parentSchemaSp, colName, newName)
	parentSchemaSp = renameColumnNameInSpannerColDefs(parentSchemaSp, colName, newName)
	parentSchemaSp = renameColumnNameInSpannerPK(parentSchemaSp, colName, newName)
	parentSchemaSp = renameColumnNameInSpannerSecondaryIndex(parentSchemaSp, colName, newName)
	parentSchemaSp = renameColumnNameInSpannerForeignkeyColumns(parentSchemaSp, colName, newName)
	parentSchemaSp = renameColumnNameInSpannerForeignkeyReferColumns(parentSchemaSp, colName, newName)

	renameColumnNameInToSpannerToSource(parentschemaTable, colName, newName, Conv)
	renameColumnNameInSpannerSchemaIssue(parentschemaTable, colName, newName, Conv)

	Conv.SpSchema[parentschemaTable] = parentSchemaSp
}

// renameColumnNameInchildTableSchema renames given column in Child Table Schema.
func renameColumnNameInchildTableSchema(Conv *internal.Conv, childschemaTable string, colName string, newName string) {

	childSchemaSp := Conv.SpSchema[childschemaTable]

	childSchemaSp = renameColumnNameInSpannerColNames(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerColDefs(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerPK(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerSecondaryIndex(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerForeignkeyColumns(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

	renameColumnNameInToSpannerToSource(childschemaTable, colName, newName, Conv)
	renameColumnNameInSpannerSchemaIssue(childschemaTable, colName, newName, Conv)

	Conv.SpSchema[childschemaTable] = childSchemaSp
}

// renameColumnNameInSpannerColNames renames given column in ColNames.
func renameColumnNameInSpannerColNames(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step I
	// update sp.ColNames

	for i, col := range sp.ColNames {
		if col == colName {
			sp.ColNames[i] = newName
			break
		}
	}

	return sp

}

// renameColumnNameInSpannerColDefs renames given column in Spanner Table ColDefs.
func renameColumnNameInSpannerColDefs(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step II
	// update sp.ColDefs

	if _, found := sp.ColDefs[colName]; found {

		sp.ColDefs[newName] = ddl.ColumnDef{
			Name:    newName,
			T:       sp.ColDefs[colName].T,
			NotNull: sp.ColDefs[colName].NotNull,
			Comment: sp.ColDefs[colName].Comment,
			Id:      sp.ColDefs[colName].Id,
		}

		delete(sp.ColDefs, colName)
	}

	return sp
}

// renameColumnNameInSpannerPK renames given column in Spanner Table Primary Key List.
func renameColumnNameInSpannerPK(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step III
	// update sp.Pks

	for i, pk := range sp.Pks {
		if pk.Col == colName {

			sp.Pks[i].Col = newName

			break
		}
	}

	return sp
}

// renameColumnNameInSpannerSecondaryIndex renames given column in Spanner Table Secondary Index List.
func renameColumnNameInSpannerSecondaryIndex(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step IV
	// update sp.Indexes

	for i, index := range sp.Indexes {
		for j, key := range index.Keys {
			if key.Col == colName {

				sp.Indexes[i].Keys[j].Col = newName

				break
			}
		}
	}

	return sp
}

// renameColumnNameInSpannerForeignkeyColumns renames given column in Spanner Table Foreignkey Columns List.
func renameColumnNameInSpannerForeignkeyColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step V
	// update sp.Fks

	for i, fk := range sp.Fks {
		for j, column := range fk.Columns {
			if column == colName {

				sp.Fks[i].Columns[j] = newName

			}
		}
	}

	return sp
}

// renameColumnNameInSpannerForeignkeyReferColumns renames given column in Spanner Table Foreignkey Refer Columns List.
func renameColumnNameInSpannerForeignkeyReferColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {

	// step VI
	// update sp.Fks.ReferColumns

	for i, fk := range sp.Fks {
		for j, column := range fk.ReferColumns {

			if column == colName {

				sp.Fks[i].ReferColumns[j] = newName

			}

		}
	}
	return sp
}

// renameColumnNameInToSpannerToSource renames given column in ToSpanner and ToSource List.
func renameColumnNameInToSpannerToSource(table string, colName string, newName string, Conv *internal.Conv) {

	srcTableName := Conv.ToSource[table].Name

	srcColName := Conv.ToSource[table].Cols[colName]

	Conv.ToSpanner[srcTableName].Cols[srcColName] = newName
	Conv.ToSource[table].Cols[newName] = srcColName
	delete(Conv.ToSource[table].Cols, colName)

}

// renameColumnNameInSpannerSchemaIssue renames given column in ToSpanner and ToSource List.
func renameColumnNameInSpannerSchemaIssue(table string, colName string, newName string, Conv *internal.Conv) {

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
