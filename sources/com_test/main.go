package main

import (
	"encoding/json"
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/mysql"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/transformation"
)

func main() {
	jsonData := `{
		"Transformations": [
			{
				"Id": "t18",
				"Name": "rule1",
				"Type": "apply_data_transformation",
				"ObjectType": "Table",
				"AssociatedObjects": "t1",
				"Enabled": true,
				"AddedOn": {
					"TimeOffset": null
				},
				"Function": "round",
				"Input": [
					{
						"Type": "source-column",
						"Value": "c1"
					},
					{
						"Datatype": "INT64",
						"Type": "static",
						"Value": "2"
					}
				],
				"Action": "writeToColumn",
				"ActionConfig": {
					"column": "c1"
				}
			}
			]
		}`
	// Define a struct to hold the JSON data
	type Payload struct {
		Transformations []internal.Transformation `json:"Transformations"`
	}

	// Unmarshal the JSON into the struct
	var payload Payload
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return
	}
	// Sample data for testing
	cvtCols := []string{"c1", "c2"}
	cvtVals := []interface{}{10, 0}
	colNameIdMap := map[string]string{
		"c1": "60.3426",
		"c2": "2",
	}
	// Access the transformation data
	t := payload.Transformations
	conv := internal.MakeConv()
	conv.Transformations = t
	conv.SpSchema = ddl.Schema{
		"t1": {
			Name:   "table",
			Id:     "t1",
			ColIds: []string{"c1", "c2"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1": {Name: "col", Id: "c1", T: ddl.Type{Name: ddl.Float64}}},
		},
	}
	conv.SrcSchema = map[string]schema.Table{
		"t1": {
			Name:   "table",
			Id:     "t1",
			ColIds: []string{"c1", "c2"},
			ColDefs: map[string]schema.Column{
				"c1": {Name: "col", Id: "c1", Type: schema.Type{Name: "float"}}},
		},
	}

	_, x, err := transformation.ProcessTransformation(conv, "t1", cvtCols, cvtVals, colNameIdMap, mysql.InfoSchemaImpl{}.GetToDdl())
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(x)
	}
}
