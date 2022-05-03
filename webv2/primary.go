package webv2

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

type PrimaryKeyRequest struct {
	TableId      int      `json:"TableId"`
	Columns      []Column `json:"Columns"`
	PrimaryKeyId int      `json:"PrimaryKeyId"`
}

type PrimaryKeyResponse struct {
	TableId      int      `json:"TableId"`
	Columns      []Column `json:"Columns"`
	PrimaryKeyId int      `json:"PrimaryKeyId"`
	Synth        string   `json:"Synth"`
}

type Column struct {
	ColumnId int    `json:"ColumnId"`
	Desc     bool   `json:"Desc"`
	ColName  string `json:"ColName"`
	Order    int    `json:"Order"`
}

func UpdatePrimaryKeyV1(w http.ResponseWriter, r *http.Request) {

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	pkeyrequest := PrimaryKeyRequest{}

	err = json.Unmarshal(reqBody, &pkeyrequest)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()

	spannertable := getspannertable(sessionState, pkeyrequest)

	var iterpklist []ddl.IndexKey

	iterpklist = spannertable.Pks

	var newerpklist []ddl.IndexKey

	fmt.Println("before spannertable primary keys :")
	fmt.Println(spannertable.Pks)
	fmt.Println("")

	for i := 0; i < len(pkeyrequest.Columns); i++ {

		for j := 0; j < len(iterpklist); j++ {

			fmt.Println("pklist[j].Col :", iterpklist[j].Col)

			id := getcolumnid(spannertable, iterpklist[j].Col)

			fmt.Println("getcolumnid :", id)

			if pkeyrequest.Columns[i].ColumnId == id {

				//update logic
				fmt.Println("")
				fmt.Println("if condtion")
				fmt.Println("pkeyrequest.Columns[i].ColumnId:", pkeyrequest.Columns[i].ColumnId)
				fmt.Println("spannertable Id :", id)
				fmt.Println("")
			} else {
				fmt.Println("")
				fmt.Println("else condtion")
				fmt.Println("pkeyrequest.Columns[i].ColumnId:", pkeyrequest.Columns[i].ColumnId)
				fmt.Println("spannertable Id :", id)
				fmt.Println("")

				pkey := ddl.IndexKey{}
				pkey.Col = getcolumnname(spannertable, pkeyrequest.Columns[i].ColumnId)
				pkey.Desc = pkeyrequest.Columns[i].Desc

				newerpklist = append(newerpklist, pkey)

			}

		}

	}

	fmt.Println("newerpklist :", newerpklist)

	pKeyResponse := PrimaryKeyResponse{}

	pKeyResponse.TableId = pkeyrequest.TableId
	pKeyResponse.PrimaryKeyId = pkeyrequest.PrimaryKeyId

	//fmt.Println("to send response from spannertable.Pks")

	spannertable.Pks = newerpklist

	for _, value := range newerpklist {

		spannertable.Pks = append(spannertable.Pks, value)
	}

	fmt.Println("spannertable primary keys :", spannertable.Pks)

	for _, indexkey := range spannertable.Pks {

		responsecolumn := Column{}

		id := getcolumnid(spannertable, indexkey.Col)
		responsecolumn.ColumnId = id
		responsecolumn.ColName = indexkey.Col
		responsecolumn.Desc = indexkey.Desc

		pKeyResponse.Columns = append(pKeyResponse.Columns, responsecolumn)

	}

	for _, table := range sessionState.Conv.SpSchema {

		if pkeyrequest.TableId == table.Id {

			sessionState.Conv.SpSchema[table.Name] = spannertable

		}
	}

	fmt.Println("pKeyResponse :", pKeyResponse)
	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(pKeyResponse)
}

func getspannertable(sessionState *session.SessionState, pkeyrequest PrimaryKeyRequest) ddl.CreateTable {

	var spannertable ddl.CreateTable

	for _, table := range sessionState.Conv.SpSchema {

		if pkeyrequest.TableId == table.Id {

			spannertable = table
		}
	}

	return spannertable
}

func getcolumnid(spannertable ddl.CreateTable, columnname string) int {

	var id int

	for _, col := range spannertable.ColDefs {
		if col.Name == columnname {
			id = col.Id

			//fmt.Println("getcolumnid :", id)

		}
	}

	return id
}

func getcolumnname(spannertable ddl.CreateTable, columnid int) string {

	var columnname string
	for _, col := range spannertable.ColDefs {
		if col.Id == columnid {
			columnname = col.Name
		}
	}

	//fmt.Println("columnname :", columnname)
	return columnname
}

/*
	for pkeyrequestcolumnindex, pkeyrequestcolumn := range pkeyrequest.Columns {

		for currentprimarykeyindex, currentprimarykey := range spannertable.Pks {

			id := getcolumnid(spannertable, currentprimarykey.Col)

			if pkeyrequestcolumn.ColumnId == id {

				fmt.Println("pkeyrequestcolumnindex :", pkeyrequestcolumnindex)

				fmt.Println("currentprimarykeyindex :", currentprimarykeyindex)

				fmt.Println("primary key exits :", currentprimarykey.Col)
				fmt.Println("case for update primary key desc", pkeyrequestcolumn.ColumnId)
				fmt.Println("$$$$$$$$$$")
				fmt.Println("")
			} else {

				fmt.Println("pkeyrequestcolumnindex :", pkeyrequestcolumnindex)

				fmt.Println("currentprimarykeyindex :", currentprimarykeyindex)
				fmt.Println("case to add new primary key :")
				fmt.Println("$$$$$$$$$$")
				fmt.Println("")
			}
		}

	}
*/

func checkprimarykeyexists(columnId int, spannertable ddl.CreateTable) bool {

	columnname := getcolumnname(spannertable, columnId)

	for _, column := range spannertable.Pks {

		if column.Col == columnname {

			fmt.Println("checkprimarykeyexists columnname :", columnname)
			fmt.Println("checkprimarykeyexists columnId :", columnId)

			return true
		}
	}
	return false
}

func UpdatePrimaryKeyV2(w http.ResponseWriter, r *http.Request) {

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	pkeyrequest := PrimaryKeyRequest{}

	err = json.Unmarshal(reqBody, &pkeyrequest)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()

	spannertable := getspannertable(sessionState, pkeyrequest)

	for i := 0; i < len(pkeyrequest.Columns); i++ {

		for j := 0; j < len(spannertable.Pks); j++ {

			id := getcolumnid(spannertable, spannertable.Pks[j].Col)

			if pkeyrequest.Columns[i].ColumnId == id {

				fmt.Println("spannertable colname :", spannertable.Pks[j].Col)

				spannertable.Pks[j].Desc = pkeyrequest.Columns[i].Desc
				spannertable.Pks[j].Order = pkeyrequest.Columns[i].Order

				fmt.Println("spannertable.Pks[j].Desc :", spannertable.Pks[j].Desc)
				fmt.Println("spannertable.Pks[j].Order :", spannertable.Pks[j].Order)

			}

		}
	}

	fmt.Println("spannertable.Pk :", spannertable.Pks)

	listone := []int{}

	for i := 0; i < len(pkeyrequest.Columns); i++ {

		listone = append(listone, pkeyrequest.Columns[i].ColumnId)

	}

	fmt.Println("listone :", listone)

	listtwo := []int{}

	for i := 0; i < len(spannertable.Pks); i++ {

		cid := getcolumnid(spannertable, spannertable.Pks[i].Col)

		listtwo = append(listtwo, cid)

	}

	fmt.Println("listtwo :", listtwo)

	update := intersect(listone, listtwo)

	fmt.Println("update :", update)

	add := difference(listone, listtwo)

	fmt.Println("add :", difference(listone, listtwo))

	for _, val := range add {

		for i := 0; i < len(pkeyrequest.Columns); i++ {

			if val == pkeyrequest.Columns[i].ColumnId {

				pkey := ddl.IndexKey{}
				pkey.Col = getcolumnname(spannertable, pkeyrequest.Columns[i].ColumnId)
				pkey.Desc = pkeyrequest.Columns[i].Desc

				pkey.Order = pkeyrequest.Columns[i].Order

				spannertable.Pks = append(spannertable.Pks, pkey)

				fmt.Println("spannertable.Pks new :", spannertable.Pks)
			}

		}
	}

	remove := difference(listtwo, listone)

	fmt.Println("remove :", difference(listtwo, listone))

	for _, val := range remove {

		colname := getcolumnname(spannertable, val)

		for i := 0; i < len(spannertable.Pks); i++ {

			if spannertable.Pks[i].Col == colname {

				fmt.Println("spannertable.Pks remove :", spannertable.Pks[i].Col)

				spannertable.Pks = append(spannertable.Pks[:i], spannertable.Pks[i+1:]...)
			}

		}
	}

	listone = []int{}

	listtwo = []int{}

	pKeyResponse := PrimaryKeyResponse{}
	pKeyResponse.TableId = pkeyrequest.TableId
	pKeyResponse.PrimaryKeyId = pkeyrequest.PrimaryKeyId

	for _, indexkey := range spannertable.Pks {

		responsecolumn := Column{}

		id := getcolumnid(spannertable, indexkey.Col)
		responsecolumn.ColumnId = id
		responsecolumn.ColName = indexkey.Col
		responsecolumn.Desc = indexkey.Desc
		responsecolumn.Order = indexkey.Order

		pKeyResponse.Columns = append(pKeyResponse.Columns, responsecolumn)

	}

	//set back

	for _, table := range sessionState.Conv.SpSchema {

		if pkeyrequest.TableId == table.Id {

			sessionState.Conv.SpSchema[table.Name] = spannertable

		}
	}

	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(pKeyResponse)
}

func intersect(listone []int, listtwo []int) []int {

	hashmap := map[int]int{}

	var result []int

	for _, val := range listone {
		hashmap[val]++
	}

	for _, val := range listtwo {
		if hashmap[val] > 0 {
			result = append(result, val)
			hashmap[val]--
		}
	}
	return result
}

func difference(listone, listtwo []int) []int {

	hashmap := make(map[int]int, len(listtwo))

	for _, val := range listtwo {
		hashmap[val]++
	}

	var diff []int

	for _, val := range listone {

		_, found := hashmap[val]

		if !found {
			diff = append(diff, val)
		}
	}

	return diff
}

func removeDuplicate(list []int) []int {

	hashmap := make(map[int]bool)

	result := []int{}

	// If the key(values of the slice) is not equal
	// to the already present value in new slice (list)
	// then we append it. else we jump on another element.

	for _, value := range list {

		_, found := hashmap[value]

		if !found {
			hashmap[value] = true
			list = append(list, value)
		}
	}

	fmt.Println("result :", result)
	return result
}

func removeDuplicateValues(intSlice []int) []int {

	hashmap := make(map[int]bool)

	result := []int{}

	// If the key(values of the slice) is not equal
	// to the already present value in new slice (list)
	// then we append it. else we jump on another element.
	for _, entry := range intSlice {

		_, value := hashmap[entry]

		if !value {
			hashmap[entry] = true
			result = append(result, entry)
		}
	}

	return result
}

func getdesc(columns []Column, columnId int) bool {

	var res bool

	for _, val := range columns {

		if val.ColumnId == columnId {
			res = val.Desc
			return res
		}

	}

	return res
}
