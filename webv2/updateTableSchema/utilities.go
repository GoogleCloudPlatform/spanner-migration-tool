package updateTableSchema

import "github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"

// IsColumnPresentInColNames check column is present in colnames.
func IsColumnPresentInColNames(colNames []string, columnName string) bool {

	for _, column := range colNames {
		if column == columnName {
			return true
		}
	}

	return false
}

// GetSpannerTableDDL return Spanner Table DDL as string.
func GetSpannerTableDDL(spannerTable ddl.CreateTable) string {

	c := ddl.Config{Comments: true, ProtectIds: false}

	ddl := spannerTable.PrintCreateTable(c)

	return ddl
}
