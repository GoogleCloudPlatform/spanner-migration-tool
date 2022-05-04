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
	Synth        bool     `json:"Synth"`
}

type Column struct {
	ColumnId int    `json:"ColumnId"`
	ColName  string `json:"ColName"`
	Desc     bool   `json:"Desc"`
	Order    int    `json:"Order"`
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

	fmt.Println("before update spannertable.Pk :", spannertable.Pks)

	update(pkeyrequest, spannertable)

	fmt.Println("after update spannertable.Pk :", spannertable.Pks)

	spannertable = addandremoveprimarykey(pkeyrequest, spannertable)

	pKeyResponse := prepareresponse(pkeyrequest, spannertable)

	fmt.Println(pKeyResponse)
	//set back

	for _, table := range sessionState.Conv.SpSchema {

		if pkeyrequest.TableId == table.Id {

			sessionState.Conv.SpSchema[table.Name] = spannertable

		}
	}

	//todo
	//i) empty collection of columns
	//ii) invalid table id not in table
	// iii) invalid column id for
	//iv) duplicate

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

func update(pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) {

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
}

func addprimarykey(add []int, pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) []ddl.IndexKey {

	pklist := []ddl.IndexKey{}

	for _, val := range add {

		for i := 0; i < len(pkeyrequest.Columns); i++ {

			if val == pkeyrequest.Columns[i].ColumnId {

				pkey := ddl.IndexKey{}
				pkey.Col = getcolumnname(spannertable, pkeyrequest.Columns[i].ColumnId)

				pkey.Desc = pkeyrequest.Columns[i].Desc

				pkey.Order = pkeyrequest.Columns[i].Order

				pklist = append(pklist, pkey)

			}

		}
	}

	return pklist
}

func removeprimarykey(remove []int, spannertable ddl.CreateTable) []ddl.IndexKey {

	pklist := []ddl.IndexKey{}

	for _, val := range remove {

		colname := getcolumnname(spannertable, val)

		for i := 0; i < len(spannertable.Pks); i++ {

			if spannertable.Pks[i].Col == colname {

				fmt.Println("in removeprimarykey spannertable.Pks remove :", spannertable.Pks[i].Col)

				//	spannertable.Pks = append(spannertable.Pks[:i], spannertable.Pks[i+1:]...)

				pklist = append(spannertable.Pks[:i], spannertable.Pks[i+1:]...)
			}

		}
	}

	return pklist
}

func prepareresponse(pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) PrimaryKeyResponse {

	var pKeyResponse PrimaryKeyResponse

	pKeyResponse.TableId = pkeyrequest.TableId
	pKeyResponse.PrimaryKeyId = pkeyrequest.PrimaryKeyId

	var issyntheticpkey bool

	for i := 0; i < len(spannertable.ColNames); i++ {

		if spannertable.ColNames[i] == "synth_id" {
			issyntheticpkey = true
		}

	}

	pKeyResponse.Synth = issyntheticpkey

	for _, indexkey := range spannertable.Pks {

		responsecolumn := Column{}

		id := getcolumnid(spannertable, indexkey.Col)
		responsecolumn.ColumnId = id
		responsecolumn.ColName = indexkey.Col
		responsecolumn.Desc = indexkey.Desc
		responsecolumn.Order = indexkey.Order

		pKeyResponse.Columns = append(pKeyResponse.Columns, responsecolumn)

	}

	fmt.Println("pKeyResponse :", pKeyResponse)

	return pKeyResponse
}

//listone
func preparenewpklist(pkeyrequest PrimaryKeyRequest) []int {

	newlist := []int{}

	for i := 0; i < len(pkeyrequest.Columns); i++ {

		newlist = append(newlist, pkeyrequest.Columns[i].ColumnId)
	}

	return newlist
}

//listtwo
func prepareoldpklist(spannertable ddl.CreateTable) []int {
	oldlist := []int{}

	for i := 0; i < len(spannertable.Pks); i++ {

		cid := getcolumnid(spannertable, spannertable.Pks[i].Col)
		oldlist = append(oldlist, cid)
	}

	return oldlist
}

func addandremoveprimarykey(pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) ddl.CreateTable {

	listone := preparenewpklist(pkeyrequest)

	fmt.Println("listone :", listone)

	listtwo := prepareoldpklist(spannertable)

	fmt.Println("listtwo :", listtwo)

	add := difference(listone, listtwo)

	fmt.Println("add :", difference(listone, listtwo))

	fmt.Println("before addprimarykey :", spannertable.Pks)

	pklist := addprimarykey(add, pkeyrequest, spannertable)

	spannertable.Pks = append(spannertable.Pks, pklist...)

	fmt.Println("after addprimarykey :", spannertable.Pks)

	remove := difference(listtwo, listone)

	fmt.Println("remove :", difference(listtwo, listone))

	//here

	fmt.Println("before removeprimarykey :", spannertable.Pks)

	if len(remove) > 0 {
		rklist := removeprimarykey(remove, spannertable)
		spannertable.Pks = rklist

	}

	fmt.Println("after removeprimarykey :", spannertable.Pks)

	listone = []int{}

	listtwo = []int{}

	return spannertable
}
