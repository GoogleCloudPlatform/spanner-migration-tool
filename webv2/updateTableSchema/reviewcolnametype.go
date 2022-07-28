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

	/*
		_, ok := Conv.SpSchema[table].ColDefs[colName]

		fmt.Println("ok :", ok)

		if !ok {

			log.Println("colname not found in table")
			http.Error(w, fmt.Sprintf("colname not found in table"), http.StatusBadRequest)
			return

		}
	*/

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, err
	}

	fmt.Println("updating for sp.ColDefs[colName] ", sp.ColDefs[colName].Name)
	fmt.Println("")

	fmt.Println("its current is ", sp.ColDefs[colName].T)
	fmt.Println("")

	fmt.Println("its new type will be  ", ty)
	fmt.Println("")

	colDef := sp.ColDefs[colName]
	colDef.T = ty

	sp.ColDefs[colName] = colDef

	fmt.Println("updated type for sp.ColDefs[colName] ", sp.ColDefs[colName].Name)
	fmt.Println("")
	fmt.Println("its updated type is ", sp.ColDefs[colName].T)

	updateType := sp.ColDefs[colName].T.Name

	if len(interleaveTableSchema) > 0 {
		interleaveTableSchema = setTypetointerleaveTableSchema(interleaveTableSchema, table, colName, updateType)

	} else {
		itc := InterleaveTableSchema{}

		itc.Table = table
		itc.InterleaveColumnChanges = []InterleaveColumn{}
		ic := InterleaveColumn{}

		ic.ColumnName = colName
		ic.Type = sp.ColDefs[colName].T.Name

		itc.InterleaveColumnChanges = append(itc.InterleaveColumnChanges, ic)

		interleaveTableSchema = append(interleaveTableSchema, itc)

	}

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

			fmt.Println("")
			fmt.Println("updating type for rsp.ColDefs[colName].Name ", rsp.ColDefs[colName].Name)

			fmt.Println("")
			fmt.Println("it current type is ", rsp.ColDefs[colName].T)

			fmt.Println("")
			fmt.Println("its new type will be  ", ty)
			fmt.Println("")

			colDef := rsp.ColDefs[colName]
			colDef.T = ty

			rsp.ColDefs[colName] = colDef

			fmt.Println("updated type for sp.ColDefs[colName] ", rsp.ColDefs[colName].Name)
			fmt.Println("")
			fmt.Println("its updated type is ", rsp.ColDefs[colName].T)

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

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, err
		}

		{
			fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName].Name, childSp.ColDefs[colName].T)

			colDef := childSp.ColDefs[colName]
			colDef.T = ty

			childSp.ColDefs[colName] = colDef

			fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName].Name, childSp.ColDefs[colName].T)

			{

				if len(interleaveTableSchema) > 0 {
					interleaveTableSchema = setTypetointerleaveTableSchema(interleaveTableSchema, childSchema, colName, updateType)

				} else {
					itc := InterleaveTableSchema{}

					itc.Table = childSchema
					itc.InterleaveColumnChanges = []InterleaveColumn{}
					ic := InterleaveColumn{}

					ic.ColumnName = colName
					ic.Type = childSp.ColDefs[colName].T.Name

					itc.InterleaveColumnChanges = append(itc.InterleaveColumnChanges, ic)

					interleaveTableSchema = append(interleaveTableSchema, itc)

				}

			}
			//15
			Conv.SpSchema[childSchema] = childSp

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
			fmt.Println("updating type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

			colDef := childSp.ColDefs[colName]
			colDef.T = ty

			childSp.ColDefs[colName] = colDef

			fmt.Println("updated type for rsp.ColDefs[colName] ", childSp.ColDefs[colName], childSp.ColDefs[colName].T)

			{
				if len(interleaveTableSchema) > 0 {
					interleaveTableSchema = setTypetointerleaveTableSchema(interleaveTableSchema, childSchema, colName, updateType)

				} else {
					itc := InterleaveTableSchema{}

					itc.Table = childSchema
					itc.InterleaveColumnChanges = []InterleaveColumn{}
					ic := InterleaveColumn{}

					ic.ColumnName = colName
					ic.Type = childSp.ColDefs[colName].T.Name

					itc.InterleaveColumnChanges = append(itc.InterleaveColumnChanges, ic)

					interleaveTableSchema = append(interleaveTableSchema, itc)

				}

			}

			//16
			Conv.SpSchema[isChild] = childSp
		}

	}

	return interleaveTableSchema, nil
}

func getInterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string) int {

	for i := 0; i < len(interleaveTableSchema); i++ {

		if interleaveTableSchema[i].Table == table {
			return i
		}
	}

	return -1
}

func getInterleaveColumnChanges(interleaveColumnChanges []InterleaveColumn, colName string) int {

	for i := 0; i < len(interleaveColumnChanges); i++ {

		if interleaveColumnChanges[i].ColumnName == colName {
			return i
		}
	}

	return -1
}

func setTypetointerleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, colName string, updateType string) []InterleaveTableSchema {

	for i := 0; i < len(interleaveTableSchema); i++ {

		if interleaveTableSchema[i].Table == table {

			for j := 0; j < len(interleaveTableSchema[i].InterleaveColumnChanges); j++ {

				if interleaveTableSchema[i].InterleaveColumnChanges[j].ColumnName == colName {
					interleaveTableSchema[i].InterleaveColumnChanges[j].UpdateType = updateType

				}
			}

		}
	}

	return interleaveTableSchema

}
