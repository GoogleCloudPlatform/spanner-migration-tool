package updateTableSchema

/*

type UpdateTableSchemaResponse struct {
	DDL     string
	Changes []TableSchemaChanges
}

type TableSchemaChanges struct {
	table         string
	Columnchanges []Columnchange
}

type Columnchange struct {
	ColumnName       string
	Type             string
	UpdateColumnName string
	UpdateType       string
}

flow II confirm

ReviewTableSchema(updateTable) {DDL,changes : [
	1,2,3,4
]}

save -> updateTableSchema(updateTable)

cancel <- reset the binding


{
	DDL : "DDL",
	changes :
[
	{table : cart

	[{
		Columnname : "Columnname",
		Type : "type",
		updateColumnname : "updateColumnname"
		updateType : "updateType",
	}],
},

[{table : user

	[{
		Columnname : "Columnname",
		Type : "type",
		updateColumnname : "updateColumnname"
		updateType : "updateType",
	}],
]

*/
