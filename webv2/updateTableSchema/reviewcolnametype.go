package updateTableSchema

import (
	"fmt"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

func ReviewcolNameType(newType, table, colName string, Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, w http.ResponseWriter) (_ []InterleaveTableSchema, err error) {

	fmt.Println("ReviewcolNameType getting called")
	fmt.Println("")

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, err
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

	//interleaveTableSchema = updatetypeinterleaveTableSchema(interleaveTableSchema, table, colName, columnId, previoustype, updateType)

	interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, table, columnId, colName, previoustype, updateType)
	//13
	Conv.SpSchema[table] = sp

	//todo
	for i, _ := range sp.Fks {

		relationTable := sp.Fks[i].ReferTable

		fmt.Println("relationTable", relationTable)

		srcTableName := Conv.ToSource[relationTable].Name

		rsp, ty, err := utilities.GetType(newType, relationTable, colName, srcTableName)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, err
		}

		{

			colDef := rsp.ColDefs[colName]
			colDef.T = ty
			rsp.ColDefs[colName] = colDef
			//14
			Conv.SpSchema[relationTable] = rsp

		}

	}

	//todo
	// update interleave table relation
	isParent, childSchema := IsParent(table)

	if isParent {

		srcTableName := Conv.ToSource[childSchema].Name

		childSp, ty, err := utilities.GetType(newType, childSchema, colName, srcTableName)

		columnId := childSp.ColDefs[colName].Id

		previoustype := childSp.ColDefs[colName].T.Name

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, err
		}

		{
			fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName].Name, childSp.ColDefs[colName].T)

			colDef := childSp.ColDefs[colName]
			colDef.T = ty

			childSp.ColDefs[colName] = colDef

			updateType := childSp.ColDefs[colName].T.Name

			fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName].Name, childSp.ColDefs[colName].T)

			//15
			Conv.SpSchema[childSchema] = childSp

			//todo
			//interleaveTableSchema = updatetypeinterleaveTableSchema(interleaveTableSchema, childSchema, colName, columnId, previoustype, updateType)
			interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, childSchema, columnId, colName, previoustype, updateType)

		}

	}

	//todo
	isChild := Conv.SpSchema[table].Parent

	if isChild != "" {

		srcTableName := Conv.ToSource[isChild].Name

		childSp, ty, err := utilities.GetType(newType, isChild, colName, srcTableName)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, err
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
			Conv.SpSchema[isChild] = childSp

			//todo
			interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, isChild, columnId, colName, previoustype, updateType)

		}

	}

	return interleaveTableSchema, nil
}
