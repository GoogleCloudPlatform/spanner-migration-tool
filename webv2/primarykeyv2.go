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

func preparenewprimarykeylist(pkeyrequest PrimaryKeyRequest) []int {

	newlist := []int{}

	for i := 0; i < len(pkeyrequest.Columns); i++ {

		newlist = append(newlist, pkeyrequest.Columns[i].ColumnId)

	}

	return newlist
}

func prepareoldprimarykeylist(spannertable ddl.CreateTable) []int {

	oldlist := []int{}

	for i := 0; i < len(spannertable.Pks); i++ {

		cid := getcolumnid(spannertable, spannertable.Pks[i].Col)

		oldlist = append(oldlist, cid)

	}
	return oldlist
}
