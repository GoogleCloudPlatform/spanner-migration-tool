package updateTableSchema

import (
	"fmt"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

func ReviewcolNameType(newType, table, colName string, Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, w http.ResponseWriter) (_ []InterleaveTableSchema, err error) {

	fmt.Println("ReviewcolNameType getting called")
	fmt.Println("")

	sp := Conv.SpSchema[table]

	//todo
	for i, _ := range sp.Fks {

		err := reviewColumnNameTypeChangeOfForeignkeyTableSchema(Conv, sp, i, colName, newType, w)
		return interleaveTableSchema, err
	}

	//todo

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				fmt.Println("found")
				fmt.Println("sp.Name :", sp.Name)

				reviewColumnNameTypeChangeOfForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newType, w)

			}

		}

	}

	//todo
	// update interleave table relation
	isParent, parentSchemaTable := IsParent(table)

	if isParent {
		interleaveTableSchema, err = reviewColumnNameTypeChangeOfParentTableSchema(Conv, interleaveTableSchema, parentSchemaTable, colName, newType, w)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
	}

	//todo
	childSchemaTable := Conv.SpSchema[table].Parent

	if childSchemaTable != "" {
		interleaveTableSchema, err = reviewColumnNameTypeChangeOfChildTableSchema(Conv, interleaveTableSchema, childSchemaTable, colName, newType, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
	}

	reviewColumnNameTypeChangeOfCurrentTableSchema(Conv, sp, interleaveTableSchema, table, colName, newType, parentSchemaTable, childSchemaTable, w)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	return interleaveTableSchema, nil
}

// reviewColumnNameTypeChangeOfForeignkeyTableSchema reviews columname type to given newType.
func reviewColumnNameTypeChangeOfForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newType string, w http.ResponseWriter) error {

	relationTable := sp.Fks[index].ReferTable

	fmt.Println("relationTable", relationTable)

	srcTableName := Conv.ToSource[relationTable].Name

	rsp, ty, err := utilities.GetType(newType, relationTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	{

		colDef := rsp.ColDefs[colName]
		colDef.T = ty
		rsp.ColDefs[colName] = colDef
		//14
		Conv.SpSchema[relationTable] = rsp

	}

	return nil
}

// reviewColumnNameTypeChangeOfForeignkeyReferTableSchema reviews columname type to given newType.
func reviewColumnNameTypeChangeOfForeignkeyReferTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	colDef := sp.ColDefs[colName]
	colDef.T = ty

	sp.ColDefs[colName] = colDef

	fmt.Println("updated type for sp.ColDefs[colName] ", sp.ColDefs[colName].Name)
	fmt.Println("")
	fmt.Println("its updated type is ", sp.ColDefs[colName].T)

	return nil
}

// reviewColumnNameTypeChangeOfParentTableSchema reviews columname type to given newType.
func reviewColumnNameTypeChangeOfParentTableSchema(Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, parentschemaTable string, colName string, newType string, w http.ResponseWriter) ([]InterleaveTableSchema, error) {

	srcTableName := Conv.ToSource[parentschemaTable].Name

	childSp, ty, err := utilities.GetType(newType, parentschemaTable, colName, srcTableName)

	columnId := childSp.ColDefs[colName].Id

	previoustype := childSp.ColDefs[colName].T.Name

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	{
		fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName].Name, childSp.ColDefs[colName].T)

		colDef := childSp.ColDefs[colName]
		colDef.T = ty

		childSp.ColDefs[colName] = colDef

		updateType := childSp.ColDefs[colName].T.Name

		fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName].Name, childSp.ColDefs[colName].T)

		Conv.SpSchema[parentschemaTable] = childSp

		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, parentschemaTable, columnId, colName, previoustype, updateType)

	}
	return interleaveTableSchema, nil
}

// reviewColumnNameTypeChangeOfChildTableSchema reviews columname type to gieven newType.
func reviewColumnNameTypeChangeOfChildTableSchema(Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, childSchemaTable string, colName string, newType string, w http.ResponseWriter) ([]InterleaveTableSchema, error) {
	srcTableName := Conv.ToSource[childSchemaTable].Name

	childSp, ty, err := utilities.GetType(newType, childSchemaTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	{

		columnId := childSp.ColDefs[colName].Id

		previoustype := childSp.ColDefs[colName].T.Name

		fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		colDef := childSp.ColDefs[colName]
		colDef.T = ty

		childSp.ColDefs[colName] = colDef

		updateType := childSp.ColDefs[colName].T.Name

		fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)
		//16
		Conv.SpSchema[childSchemaTable] = childSp

		//todo
		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, childSchemaTable, columnId, colName, previoustype, updateType)

	}

	return interleaveTableSchema, nil

}

// reviewColumnNameTypeChangeOfCurrentTableSchema reviews columname type to gieven newType.
func reviewColumnNameTypeChangeOfCurrentTableSchema(Conv *internal.Conv, sp ddl.CreateTable, interleaveTableSchema []InterleaveTableSchema, table string, colName string, newType string, parentSchemaTable string, childSchemaTable string, w http.ResponseWriter) ([]InterleaveTableSchema, error) {

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	columnId := sp.ColDefs[colName].Id

	previoustype := sp.ColDefs[colName].T.Name

	fmt.Println("previoustype :", previoustype)
	fmt.Println("")

	colDef := sp.ColDefs[colName]
	colDef.T = ty

	sp.ColDefs[colName] = colDef

	fmt.Println("updated type for sp.ColDefs[colName] ", sp.ColDefs[colName].Name)
	fmt.Println("")
	fmt.Println("its updated type is ", sp.ColDefs[colName].T)

	updateType := sp.ColDefs[colName].T.Name

	fmt.Println("###########################")
	fmt.Println("updateType :", updateType)
	fmt.Println("")
	fmt.Println("###########################")

	//13
	Conv.SpSchema[table] = sp

	if parentSchemaTable != "" || childSchemaTable != "" {
		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, table, columnId, colName, previoustype, updateType)
	}

	return interleaveTableSchema, nil
}
