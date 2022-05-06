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

// PrimaryKeyRequest represents  Primary keys API Payload
type PrimaryKeyRequest struct {
	TableId      int      `json:"TableId"`
	Columns      []Column `json:"Columns"`
	PrimaryKeyId int      `json:"PrimaryKeyId"`
}

/*PrimaryKeyResponse represents  Primary keys API response
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

//primaryKey updates Primary keys in Spanner Table.
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
	spannerTable, found := getSpannerTable(sessionState, pkeyrequest)

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

	if !isColumnIdValid(pkeyrequest, spannerTable) {
		log.Println(err)
		http.Error(w, fmt.Sprintf("colummId not found error : %v", err), http.StatusBadRequest)
		return

	}

	updatePrimaryKey(pkeyrequest, spannerTable)
	spannerTable = insertOrRemovePrimarykey(pkeyrequest, spannerTable)
	pKeyResponse := prepareResponse(pkeyrequest, spannerTable)

	for _, table := range sessionState.Conv.SpSchema {
		if pkeyrequest.TableId == table.Id {
			sessionState.Conv.SpSchema[table.Name] = spannerTable
		}
	}

	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(pKeyResponse)
}

func getSpannerTable(sessionState *session.SessionState, pkeyrequest PrimaryKeyRequest) (spannerTable ddl.CreateTable, found bool) {

	for _, table := range sessionState.Conv.SpSchema {

		if pkeyrequest.TableId == table.Id {
			spannerTable = table
			found = true
		}
	}
	return spannerTable, found
}

func getColumnName(spannerTable ddl.CreateTable, columnId int) string {

	var columnName string

	for _, col := range spannerTable.ColDefs {
		if col.Id == columnId {
			columnName = col.Name
		}
	}
	return columnName
}

func getColumnId(spannerTable ddl.CreateTable, columnName string) int {

	var id int

	for _, col := range spannerTable.ColDefs {
		if col.Name == columnName {
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
func updatePrimaryKey(pkeyrequest PrimaryKeyRequest, spannerTable ddl.CreateTable) {

	for i := 0; i < len(pkeyrequest.Columns); i++ {

		for j := 0; j < len(spannerTable.Pks); j++ {

			id := getColumnId(spannerTable, spannerTable.Pks[j].Col)

			if pkeyrequest.Columns[i].ColumnId == id {

				spannerTable.Pks[j].Desc = pkeyrequest.Columns[i].Desc
				spannerTable.Pks[j].Order = pkeyrequest.Columns[i].Order
			}

		}
	}
}

//addPrimaryKey insert primary key into list of IndexKey
func addPrimaryKey(add []int, pkeyrequest PrimaryKeyRequest, spannerTable ddl.CreateTable) []ddl.IndexKey {

	pklist := []ddl.IndexKey{}

	for _, val := range add {

		for i := 0; i < len(pkeyrequest.Columns); i++ {

			if val == pkeyrequest.Columns[i].ColumnId {

				pkey := ddl.IndexKey{}
				pkey.Col = getColumnName(spannerTable, pkeyrequest.Columns[i].ColumnId)
				pkey.Desc = pkeyrequest.Columns[i].Desc
				pkey.Order = pkeyrequest.Columns[i].Order

				pklist = append(pklist, pkey)
			}
		}
	}
	return pklist
}

//removePrimaryKey removed primary key from list of IndexKey
func removePrimaryKey(remove []int, spannerTable ddl.CreateTable) []ddl.IndexKey {

	pklist := []ddl.IndexKey{}

	for _, val := range remove {

		colname := getColumnName(spannerTable, val)

		for i := 0; i < len(spannerTable.Pks); i++ {

			if spannerTable.Pks[i].Col == colname {

				pklist = append(spannerTable.Pks[:i], spannerTable.Pks[i+1:]...)
			}
		}
	}
	return pklist
}

//prepareResponse prepare response for primary key api
func prepareResponse(pkeyrequest PrimaryKeyRequest, spannerTable ddl.CreateTable) PrimaryKeyResponse {

	var pKeyResponse PrimaryKeyResponse

	pKeyResponse.TableId = pkeyrequest.TableId
	pKeyResponse.PrimaryKeyId = pkeyrequest.PrimaryKeyId

	var isSynthPrimaryKey bool

	//todo check with team
	for i := 0; i < len(spannerTable.ColNames); i++ {

		if spannerTable.ColNames[i] == "synth_id" {
			isSynthPrimaryKey = true
		}
	}

	pKeyResponse.Synth = isSynthPrimaryKey

	for _, indexkey := range spannerTable.Pks {

		responseColumn := Column{}

		id := getColumnId(spannerTable, indexkey.Col)
		responseColumn.ColumnId = id
		responseColumn.ColName = indexkey.Col
		responseColumn.Desc = indexkey.Desc
		responseColumn.Order = indexkey.Order

		pKeyResponse.Columns = append(pKeyResponse.Columns, responseColumn)
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
func prepareoldpklist(spannerTable ddl.CreateTable) []int {

	oldlist := []int{}

	for i := 0; i < len(spannerTable.Pks); i++ {
		cid := getColumnId(spannerTable, spannerTable.Pks[i].Col)
		oldlist = append(oldlist, cid)
	}
	return oldlist
}

func preparecolumnlist(spannerTable ddl.CreateTable) []int {

	oldlist := []int{}

	for _, column := range spannerTable.ColDefs {
		oldlist = append(oldlist, column.Id)
	}
	return oldlist
}

/*
insertOrRemovePrimarykey performs insert or remove primary key operation based on
difference of two pkeyrequest and spannerTable.Pks.
*/
func insertOrRemovePrimarykey(pkeyrequest PrimaryKeyRequest, spannerTable ddl.CreateTable) ddl.CreateTable {

	listone := preparenewpklist(pkeyrequest)
	listtwo := prepareoldpklist(spannerTable)

	//primary key Id only presnt in pkeyrequest
	// hence new primary key add primary key into  spannerTable.Pk list
	insert := difference(listone, listtwo)
	pklist := addPrimaryKey(insert, pkeyrequest, spannerTable)

	spannerTable.Pks = append(spannerTable.Pks, pklist...)

	//primary key Id only presnt in spannertable.Pks
	// hence remove primary key from  spannertable.Pks
	remove := difference(listtwo, listone)

	if len(remove) > 0 {
		rklist := removePrimaryKey(remove, spannerTable)
		spannerTable.Pks = rklist
	}

	listone = []int{}
	listtwo = []int{}

	return spannerTable
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
