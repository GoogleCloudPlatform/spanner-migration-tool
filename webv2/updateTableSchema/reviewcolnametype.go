package updateTableSchema

import (
	"fmt"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

func ReviewcolNameType(newType, table, colName string, Conv *internal.Conv, columnchange []Columnchange, w http.ResponseWriter) (_ []Columnchange, err error) {

	//sessionState := session.GetSessionState()

	//srcTableName := sessionState.Conv.ToSource[table].Name

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, err
	}

	fmt.Println("updating type for sp.ColDefs[colName] ", sp.ColDefs[colName], sp.ColDefs[colName].T)

	colDef := sp.ColDefs[colName]
	colDef.T = ty

	sp.ColDefs[colName] = colDef

	fmt.Println("updated type for sp.ColDefs[colName] ", sp.ColDefs[colName], sp.ColDefs[colName].T)

	//13
	Conv.SpSchema[table] = sp

	//todo
	for i, _ := range sp.Fks {

		relationTable := sp.Fks[i].ReferTable

		srcTableName := Conv.ToSource[relationTable].Name

		rsp, ty, err := utilities.GetType(newType, relationTable, colName, srcTableName)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, err
		}

		fmt.Println("updating type for rsp.ColDefs[colName] ", rsp.ColDefs[colName], rsp.ColDefs[colName].T)

		colDef := rsp.ColDefs[colName]
		colDef.T = ty

		rsp.ColDefs[colName] = colDef

		fmt.Println("updated type for rsp.ColDefs[colName] ", rsp.ColDefs[colName], rsp.ColDefs[colName].T)

		//14
		Conv.SpSchema[table] = rsp
	}

	//todo
	// update interleave table relation
	isParent, childSchema := IsParent(table)

	if isParent {

		srcTableName := Conv.ToSource[childSchema].Name

		childSp, ty, err := utilities.GetType(newType, childSchema, colName, srcTableName)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, err
		}

		fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		colDef := childSp.ColDefs[colName]
		colDef.T = ty

		childSp.ColDefs[colName] = colDef

		fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		//15
		Conv.SpSchema[table] = childSp

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

		fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		colDef := childSp.ColDefs[colName]
		colDef.T = ty

		childSp.ColDefs[colName] = colDef

		fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		//16
		Conv.SpSchema[table] = childSp
	}

	return columnchange, nil
}
