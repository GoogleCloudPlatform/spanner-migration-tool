package updateTableSchema

import (
	"fmt"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

//todo check type update

func UpdatecolNameType(newType, table, colName string, Conv *internal.Conv, w http.ResponseWriter) {

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(newType, table, colName, srcTableName)

	if err != nil {
		fmt.Println("err:", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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
			fmt.Println("err")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Println("updating type for rsp.ColDefs[colName] ", rsp.ColDefs[colName], rsp.ColDefs[colName].T)

		colDef := rsp.ColDefs[colName]
		colDef.T = ty

		rsp.ColDefs[colName] = colDef

		fmt.Println("updated type for rsp.ColDefs[colName] ", rsp.ColDefs[colName], rsp.ColDefs[colName].T)

		//14
		Conv.SpSchema[relationTable] = rsp
	}

	//todo
	// update interleave table relation
	isParent, childSchema := IsParent(table)

	if isParent {

		srcTableName := Conv.ToSource[childSchema].Name

		childSp, ty, err := utilities.GetType(newType, childSchema, colName, srcTableName)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		colDef := childSp.ColDefs[colName]
		colDef.T = ty

		childSp.ColDefs[colName] = colDef

		fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		//15
		Conv.SpSchema[childSchema] = childSp

	}

	//todo
	isChild := Conv.SpSchema[table].Parent

	if isChild != "" {

		srcTableName := Conv.ToSource[isChild].Name

		childSp, ty, err := utilities.GetType(newType, isChild, colName, srcTableName)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		colDef := childSp.ColDefs[colName]
		colDef.T = ty

		childSp.ColDefs[colName] = colDef

		fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

		//16
		Conv.SpSchema[isChild] = childSp
	}
}

func UpdateNotNull(notNullChange, table, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]

	switch notNullChange {
	case "ADDED":
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = true
		sp.ColDefs[colName] = spColDef
	case "REMOVED":
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = false
		sp.ColDefs[colName] = spColDef
	}
}

func IsParent(table string) (bool, string) {
	sessionState := session.GetSessionState()

	for _, spSchema := range sessionState.Conv.SpSchema {
		if spSchema.Parent == table {
			return true, spSchema.Name
		}
	}
	return false, ""
}

func IsPartOfPK(col, table string) bool {
	sessionState := session.GetSessionState()

	for _, pk := range sessionState.Conv.SpSchema[table].Pks {
		if pk.Col == col {
			return true
		}
	}
	return false
}
