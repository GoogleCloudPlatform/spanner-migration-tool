package updateTableSchema

import (
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// ReviewColumnNameType reviews columname type to given newType.
func ReviewColumnNameType(newType, table, colName string, Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, w http.ResponseWriter) (_ []InterleaveTableSchema, err error) {

	sp := Conv.SpSchema[table]

	for i, _ := range sp.Fks {

		err := reviewColumnNameTypeChangeOfForeignkeyTableSchema(Conv, sp, i, colName, newType, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {

				reviewColumnNameTypeChangeOfForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newType, w)

			}

		}

	}

	// update interleave table relation
	isParent, parentSchemaTable := IsParent(table)

	if isParent {
		interleaveTableSchema, err = reviewColumnNameTypeChangeOfParentTableSchema(Conv, interleaveTableSchema, parentSchemaTable, colName, newType, w)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
	}

	childSchemaTable := Conv.SpSchema[table].Parent

	if childSchemaTable != "" {
		interleaveTableSchema, err = reviewColumnNameTypeChangeOfChildTableSchema(Conv, interleaveTableSchema, childSchemaTable, colName, newType, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
	}

	interleaveTableSchema, err = reviewColumnNameTypeChangeOfCurrentTableSchema(Conv, sp, interleaveTableSchema, table, colName, newType, parentSchemaTable, childSchemaTable, w)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	return interleaveTableSchema, nil
}

// reviewColumnNameTypeChangeOfForeignkeyTableSchema reviews columname type to given newType.
func reviewColumnNameTypeChangeOfForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newType string, w http.ResponseWriter) error {

	relationTable := sp.Fks[index].ReferTable

	srcTableName := Conv.ToSource[relationTable].Name

	rsp, ty, err := utilities.GetType(Conv, newType, relationTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	{

		colDef := rsp.ColDefs[colName]
		colDef.T = ty
		rsp.ColDefs[colName] = colDef
		Conv.SpSchema[relationTable] = rsp

	}

	return nil
}

// reviewColumnNameTypeChangeOfForeignkeyReferTableSchema reviews columname type to given newType.
func reviewColumnNameTypeChangeOfForeignkeyReferTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(Conv, newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	colDef := sp.ColDefs[colName]
	colDef.T = ty

	sp.ColDefs[colName] = colDef

	return nil
}

// reviewColumnNameTypeChangeOfParentTableSchema reviews columname type to given newType.
func reviewColumnNameTypeChangeOfParentTableSchema(Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, parentschemaTable string, colName string, newType string, w http.ResponseWriter) ([]InterleaveTableSchema, error) {

	srcTableName := Conv.ToSource[parentschemaTable].Name

	childSp, ty, err := utilities.GetType(Conv, newType, parentschemaTable, colName, srcTableName)

	columnId := childSp.ColDefs[colName].Id

	previoustype := childSp.ColDefs[colName].T.Name

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	{

		colDef := childSp.ColDefs[colName]
		colDef.T = ty

		childSp.ColDefs[colName] = colDef

		updateType := childSp.ColDefs[colName].T.Name

		Conv.SpSchema[parentschemaTable] = childSp

		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, parentschemaTable, columnId, colName, previoustype, updateType)

	}
	return interleaveTableSchema, nil
}

// reviewColumnNameTypeChangeOfChildTableSchema reviews columname type to gieven newType.
func reviewColumnNameTypeChangeOfChildTableSchema(Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, childSchemaTable string, colName string, newType string, w http.ResponseWriter) ([]InterleaveTableSchema, error) {
	srcTableName := Conv.ToSource[childSchemaTable].Name

	childSp, ty, err := utilities.GetType(Conv, newType, childSchemaTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	{

		columnId := childSp.ColDefs[colName].Id

		previoustype := childSp.ColDefs[colName].T.Name

		colDef := childSp.ColDefs[colName]
		colDef.T = ty

		childSp.ColDefs[colName] = colDef

		updateType := childSp.ColDefs[colName].T.Name
		Conv.SpSchema[childSchemaTable] = childSp

		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, childSchemaTable, columnId, colName, previoustype, updateType)

	}

	return interleaveTableSchema, nil

}

// reviewColumnNameTypeChangeOfCurrentTableSchema reviews columname type to gieven newType.
func reviewColumnNameTypeChangeOfCurrentTableSchema(Conv *internal.Conv, sp ddl.CreateTable, interleaveTableSchema []InterleaveTableSchema, table string, colName string, newType string, parentSchemaTable string, childSchemaTable string, w http.ResponseWriter) ([]InterleaveTableSchema, error) {

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(Conv, newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	columnId := sp.ColDefs[colName].Id

	previoustype := sp.ColDefs[colName].T.Name

	colDef := sp.ColDefs[colName]
	colDef.T = ty

	sp.ColDefs[colName] = colDef

	updateType := sp.ColDefs[colName].T.Name

	Conv.SpSchema[table] = sp

	if parentSchemaTable != "" || childSchemaTable != "" {
		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, table, columnId, colName, previoustype, updateType)
	}

	return interleaveTableSchema, nil
}
