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

/*
PrimaryKeyRequest represents  Primary keys API Payload
*/
type PrimaryKeyRequest struct {
	TableId      int      `json:"TableId"`
	Columns      []Column `json:"Columns"`
	PrimaryKeyId int      `json:"PrimaryKeyId"`
}

/*
PrimaryKeyResponse represents  Primary keys API response
Synth is true is for table Primary Key Id is not present and it is generated
*/
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

/*
primaryKey updates Primary keys in Spanner Table.
*/
func primaryKey(w http.ResponseWriter, r *http.Request) {

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

	spannertable, found := getSpannerTable(sessionState, pkeyrequest)

	if !found {
		log.Println(err)
		http.Error(w, fmt.Sprintf("tableId not found : %v", err), http.StatusNotFound)
		return

	}

	if len(pkeyrequest.Columns) == 0 {
		log.Println(err)
		http.Error(w, fmt.Sprintf("empty columm error : %v", err), http.StatusBadRequest)
		return

	}

	if !isColumnIdValid(pkeyrequest, spannertable) {
		log.Println(err)
		http.Error(w, fmt.Sprintf("colummId not found error : %v", err), http.StatusBadRequest)
		return

	}

	updateprimaryKey(pkeyrequest, spannertable)
	spannertable = insertOrRemovePrimarykey(pkeyrequest, spannertable)
	pKeyResponse := prepareResponse(pkeyrequest, spannertable)

	for _, table := range sessionState.Conv.SpSchema {

		if pkeyrequest.TableId == table.Id {

			sessionState.Conv.SpSchema[table.Name] = spannertable

		}
	}

	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(pKeyResponse)
}

func getSpannerTable(sessionState *session.SessionState, pkeyrequest PrimaryKeyRequest) (spannertable ddl.CreateTable, found bool) {

	for _, table := range sessionState.Conv.SpSchema {

		if pkeyrequest.TableId == table.Id {
			spannertable = table
			found = true
		}
	}
	return spannertable, found
}

func getColumnName(spannertable ddl.CreateTable, columnid int) string {

	var columnname string

	for _, col := range spannertable.ColDefs {
		if col.Id == columnid {
			columnname = col.Name
		}
	}
	return columnname
}

func getColumnId(spannertable ddl.CreateTable, columnname string) int {

	var id int

	for _, col := range spannertable.ColDefs {
		if col.Name == columnname {
			id = col.Id
		}
	}
	return id
}

/*
difference gives list of element that are only present in first list
*/
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

//updateprimaryKey updates primary key desc and order for primaryKey.
func updateprimaryKey(pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) {

	for i := 0; i < len(pkeyrequest.Columns); i++ {

		for j := 0; j < len(spannertable.Pks); j++ {

			id := getColumnId(spannertable, spannertable.Pks[j].Col)

			if pkeyrequest.Columns[i].ColumnId == id {

				spannertable.Pks[j].Desc = pkeyrequest.Columns[i].Desc
				spannertable.Pks[j].Order = pkeyrequest.Columns[i].Order
			}

		}
	}
}

//addPrimaryKey insert primary key into list of IndexKey
func addPrimaryKey(add []int, pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) []ddl.IndexKey {

	pklist := []ddl.IndexKey{}

	for _, val := range add {

		for i := 0; i < len(pkeyrequest.Columns); i++ {

			if val == pkeyrequest.Columns[i].ColumnId {

				pkey := ddl.IndexKey{}
				pkey.Col = getColumnName(spannertable, pkeyrequest.Columns[i].ColumnId)
				pkey.Desc = pkeyrequest.Columns[i].Desc
				pkey.Order = pkeyrequest.Columns[i].Order

				pklist = append(pklist, pkey)
			}
		}
	}
	return pklist
}

//removePrimaryKey removed primary key from list of IndexKey
func removePrimaryKey(remove []int, spannertable ddl.CreateTable) []ddl.IndexKey {

	pklist := []ddl.IndexKey{}

	for _, val := range remove {

		colname := getColumnName(spannertable, val)

		for i := 0; i < len(spannertable.Pks); i++ {

			if spannertable.Pks[i].Col == colname {

				pklist = append(spannertable.Pks[:i], spannertable.Pks[i+1:]...)
			}
		}
	}
	return pklist
}

//prepareResponse prepare response for primary key api
func prepareResponse(pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) PrimaryKeyResponse {

	var pKeyResponse PrimaryKeyResponse

	pKeyResponse.TableId = pkeyrequest.TableId
	pKeyResponse.PrimaryKeyId = pkeyrequest.PrimaryKeyId

	var issyntheticpkey bool

	//todo check with team
	for i := 0; i < len(spannertable.ColNames); i++ {

		if spannertable.ColNames[i] == "synth_id" {
			issyntheticpkey = true
		}
	}

	pKeyResponse.Synth = issyntheticpkey

	for _, indexkey := range spannertable.Pks {

		responsecolumn := Column{}

		id := getColumnId(spannertable, indexkey.Col)
		responsecolumn.ColumnId = id
		responsecolumn.ColName = indexkey.Col
		responsecolumn.Desc = indexkey.Desc
		responsecolumn.Order = indexkey.Order

		pKeyResponse.Columns = append(pKeyResponse.Columns, responsecolumn)
	}
	return pKeyResponse
}

//preparenewpklist prepare first list for difference
func preparenewpklist(pkeyrequest PrimaryKeyRequest) []int {

	newlist := []int{}

	for i := 0; i < len(pkeyrequest.Columns); i++ {
		newlist = append(newlist, pkeyrequest.Columns[i].ColumnId)
	}
	return newlist
}

//prepareoldpklist prepare second list for difference
func prepareoldpklist(spannertable ddl.CreateTable) []int {

	oldlist := []int{}

	for i := 0; i < len(spannertable.Pks); i++ {
		cid := getColumnId(spannertable, spannertable.Pks[i].Col)
		oldlist = append(oldlist, cid)
	}
	return oldlist
}

func preparecolumnlist(spannertable ddl.CreateTable) []int {

	oldlist := []int{}

	for _, column := range spannertable.ColDefs {
		oldlist = append(oldlist, column.Id)
	}
	return oldlist
}

/*
insertOrRemovePrimarykey performs insert or remove primary key operation based on
difference of two pkeyrequest and spannertable.Pks.
*/
func insertOrRemovePrimarykey(pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) ddl.CreateTable {

	listone := preparenewpklist(pkeyrequest)
	listtwo := prepareoldpklist(spannertable)

	//primary key Id only presnt in pkeyrequest
	// hence new primary key add primary key into  spannertable.Pk list
	insert := difference(listone, listtwo)
	pklist := addPrimaryKey(insert, pkeyrequest, spannertable)

	spannertable.Pks = append(spannertable.Pks, pklist...)

	//primary key Id only presnt in spannertable.Pks
	// hence remove primary key from  spannertable.Pks
	remove := difference(listtwo, listone)

	if len(remove) > 0 {
		rklist := removePrimaryKey(remove, spannertable)
		spannertable.Pks = rklist
	}

	listone = []int{}
	listtwo = []int{}

	return spannertable
}

func isColumnIdValid(pkeyrequest PrimaryKeyRequest, spannertable ddl.CreateTable) bool {

	var validColumnId bool

	listone := preparenewpklist(pkeyrequest)
	listtwo := preparecolumnlist(spannertable)

	leftjoin := difference(listone, listtwo)

	if len(leftjoin) > 0 {
		validColumnId = false
		return validColumnId
	}

	return true
}
