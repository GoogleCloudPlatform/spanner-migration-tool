package updateTableSchema

import (
	"fmt"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

func UpdateColNameType(newType, table, colName string, Conv *internal.Conv, w http.ResponseWriter) {

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

		err = UpdateColNameTypeForeignkeyTableSchema(Conv, sp, i, colName, newType, w)

		if err != nil {
			return
		}
	}

	//todo

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				fmt.Println("found")
				fmt.Println("sp.Name :", sp.Name)

				UpdateColNameTypeForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newType, w)

			}

		}

	}

	//todo
	// update interleave table relation
	isParent, parentschemaTable := IsParent(table)

	if isParent {

		err = UpdateColNameTypeParentschemaTable(Conv, parentschemaTable, colName, newType, w)
		if err != nil {
			return
		}
	}

	//todo
	childSchemaTable := Conv.SpSchema[table].Parent

	if childSchemaTable != "" {

		err = UpdateColNameTypechildschemaTable(Conv, childSchemaTable, colName, newType, w)
		if err != nil {
			return
		}
	}
}

func UpdateColNameTypeForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newType string, w http.ResponseWriter) error {

	relationTable := sp.Fks[index].ReferTable

	srcTableName := Conv.ToSource[relationTable].Name

	rsp, ty, err := utilities.GetType(newType, relationTable, colName, srcTableName)

	if err != nil {
		fmt.Println("err")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	fmt.Println("updating type for rsp.ColDefs[colName] ", rsp.ColDefs[colName], rsp.ColDefs[colName].T)

	colDef := rsp.ColDefs[colName]
	colDef.T = ty

	rsp.ColDefs[colName] = colDef

	fmt.Println("updated type for rsp.ColDefs[colName] ", rsp.ColDefs[colName], rsp.ColDefs[colName].T)

	//14
	Conv.SpSchema[relationTable] = rsp

	return nil

}

func UpdateColNameTypeForeignkeyReferTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string, newType string, w http.ResponseWriter) error {

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

func UpdateColNameTypeParentschemaTable(Conv *internal.Conv, parentschemaTable string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := Conv.ToSource[parentschemaTable].Name

	childSp, ty, err := utilities.GetType(newType, parentschemaTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

	colDef := childSp.ColDefs[colName]
	colDef.T = ty

	childSp.ColDefs[colName] = colDef

	fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

	//15
	Conv.SpSchema[parentschemaTable] = childSp

	return nil
}

func UpdateColNameTypechildschemaTable(Conv *internal.Conv, childSchemaTable string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := Conv.ToSource[childSchemaTable].Name

	childSp, ty, err := utilities.GetType(newType, childSchemaTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

	colDef := childSp.ColDefs[colName]
	colDef.T = ty

	childSp.ColDefs[colName] = colDef

	fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

	//16
	Conv.SpSchema[childSchemaTable] = childSp

	return nil
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
