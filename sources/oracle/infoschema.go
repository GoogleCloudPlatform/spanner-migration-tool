package oracle

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type InfoSchemaImpl struct {
	DbName string
	Db     *sql.DB
}

// GetToDdl function below implement the common.InfoSchema interface.
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// GetTableName returns table name.
func (isi InfoSchemaImpl) GetTableName(dbName string, tableName string) string {
	return tableName
}

// GetRowsFromTable returns a sql Rows object for a table.
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error) {
	panic("unimplemented")
}

// ProcessData performs data conversion for source database.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, srcTable string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable) error {
	panic("unimplemented")
}

func ProcessDataRow(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, values []string) {
	panic("unimplemented")
}

// GetRowCount with number of rows in each table.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	panic("unimplemented")
}

func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	q := fmt.Sprintf("SELECT table_name from all_tables where owner = '%s'", isi.DbName)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}
	defer rows.Close()
	var tableName string
	var tables []common.SchemaAndName
	for rows.Next() {
		rows.Scan(&tableName)
		tables = append(tables, common.SchemaAndName{Schema: isi.DbName, Name: tableName})
	}
	return tables, nil
}

// GetColumns returns a list of Column objects and names
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	q := fmt.Sprintf(`SELECT column_name, data_type, nullable, data_default, data_length, data_precision, data_scale FROM USER_TAB_COLUMNS WHERE table_name = '%s'`, table.Name)
	cols, err := isi.Db.Query(q)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colDefs := make(map[string]schema.Column)
	var colNames []string
	var colName, dataType string
	var isNullable string
	var colDefault sql.NullString
	var charMaxLen, numericPrecision, numericScale sql.NullInt64
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
			// Type of constraint definition in oracle C (check constraint on a table)
			// P (primary key), U (unique key) ,R (referential integrity), V (with check option, on a view)
			// O (with read only, on a view).
			// We've already filtered out PRIMARY KEY.
			switch c {
			case "C":
				ignored.Check = true
			case "R", "P", "U":
				// Nothing to do here -- these are handled elsewhere.
			}
		}
		ignored.Default = colDefault.Valid
		c := schema.Column{
			Name:    colName,
			Type:    toType(dataType, charMaxLen, numericPrecision, numericScale),
			NotNull: strings.ToUpper(isNullable) == "N",
			Ignored: ignored,
		}
		colDefs[colName] = c
		colNames = append(colNames, colName)
	}
	return colDefs, colNames, nil
}

// GetConstraints returns a list of primary keys and by-column map of
// other constraints.  Note: we need to preserve ordinal order of
// columns in primary key constraints.
// Note that foreign key constraints are handled in getForeignKeys.
func (isi InfoSchemaImpl) GetConstraints(conv *internal.Conv, table common.SchemaAndName) ([]string, map[string][]string, error) {
	q := fmt.Sprintf(`SELECT k.column_name,t.constraint_type
	   FROM ALL_CONSTRAINTS t
       INNER JOIN ALL_CONS_COLUMNS k
       ON (k.constraint_name = t.constraint_name) WHERE k.table_name = '%s'`, table.Name)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var primaryKeys []string
	var col, constraint string
	m := make(map[string][]string)
	for rows.Next() {
		err := rows.Scan(&col, &constraint)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if col == "" || constraint == "" {
			conv.Unexpected("Got empty col or constraint")
			continue
		}
		// P (primary key) constraint in oracle
		switch constraint {
		case "P":
			primaryKeys = append(primaryKeys, col)
		default:
			m[col] = append(m[col], constraint)
		}
	}
	return primaryKeys, m, nil
}

// GetForeignKeys return list all the foreign keys constraints.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	q := fmt.Sprintf(`SELECT B.TABLE_NAME AS REF_TABLE, A.COLUMN_NAME AS COL_NAME,
				B.COLUMN_NAME AS REF_COL_NAME ,A.CONSTRAINT_NAME AS NAME
		FROM ALL_CONS_COLUMNS A 
		JOIN ALL_CONSTRAINTS C ON A.OWNER = C.OWNER 
			AND A.CONSTRAINT_NAME = C.CONSTRAINT_NAME
    	JOIN ALL_CONS_COLUMNS B ON B.OWNER = C.OWNER 
			AND B.CONSTRAINT_NAME = C.R_CONSTRAINT_NAME
    	WHERE A.TABLE_NAME='%s' AND A.OWNER='%s'`, table.Name, isi.DbName)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var col, refCol, refTable, fKeyName string
	fKeys := make(map[string]common.FkConstraint)
	var keyNames []string

	for rows.Next() {
		err := rows.Scan(&refTable, &col, &refCol, &fKeyName)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.Cols = append(fk.Cols, col)
			fk.Refcols = append(fk.Refcols, refCol)
			fKeys[fKeyName] = fk
			continue
		}
		fKeys[fKeyName] = common.FkConstraint{Name: fKeyName, Table: refTable, Refcols: []string{refCol}, Cols: []string{col}}
		keyNames = append(keyNames, fKeyName)
	}
	sort.Strings(keyNames)
	for _, k := range keyNames {
		foreignKeys = append(foreignKeys,
			schema.ForeignKey{
				Name:         fKeys[k].Name,
				Columns:      fKeys[k].Cols,
				ReferTable:   fKeys[k].Table,
				ReferColumns: fKeys[k].Refcols})
	}
	return foreignKeys, nil
}

// GetIndexes return a list of all indexes for the specified table.
// Oracle db support several types of index: 1. Normal indexes. (By default, Oracle Database creates B-tree indexes.)
// 2.Bitmap indexes 3.Partitioned indexes 4. Function-based indexes 5.Domain indexes,
// we are only considering normal index as of now.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName) ([]schema.Index, error) {
	q := fmt.Sprintf(`SELECT IC.INDEX_NAME,IC.COLUMN_NAME,IC.COLUMN_POSITION, 
					IC.DESCEND,I.UNIQUENESS, IE.COLUMN_EXPRESSION, I.INDEX_TYPE 
                FROM  ALL_IND_COLUMNS IC 
				LEFT JOIN ALL_IND_EXPRESSIONS IE 
               		ON IC.INDEX_NAME = IE.INDEX_NAME AND IC.COLUMN_POSITION=IE.COLUMN_POSITION
                LEFT JOIN ALL_INDEXES I 
			   		ON IC.INDEX_NAME = I.INDEX_NAME
                 WHERE IC.INDEX_OWNER='%s' AND IC.TABLE_NAME  = '%s'
            	 ORDER BY IC.INDEX_NAME, IC.COLUMN_POSITION`, table.Schema, table.Name)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, column, sequence, Unique, indexType string
	var collation, colexpression sql.NullString
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	ignoredIndex := make(map[string]bool)
	var indexes []schema.Index
	for rows.Next() {
		if err := rows.Scan(&name, &column, &sequence, &collation, &Unique, &colexpression, &indexType); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		// ingnore all index except normal
		// UPPER("EMAIL") check for the function call with "(",")"
		if indexType != "NORMAL" && strings.Contains(colexpression.String, "(") && strings.Contains(colexpression.String, ")") {
			ignoredIndex[name] = true
		}

		//INDEX1_LAST	SYS_NC00009$	1	DESC	NONUNIQUE	"LAST_NAME"	FUNCTION-BASED NORMAL
		// DESC column make index functional index but as special case we included that
		// and update column name with column expression
		if colexpression.Valid && !strings.Contains(colexpression.String, "(") && !strings.Contains(colexpression.String, ")") {
			column = colexpression.String[1 : len(colexpression.String)-1]
		}

		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{Name: name, Unique: (Unique == "UNIQUE")}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{Column: column, Desc: (collation.Valid && collation.String == "DESC")})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		// only add noraml index
		if _, found := ignoredIndex[k]; !found {
			indexes = append(indexes, indexMap[k])
		}
	}
	return indexes, nil
}

func toType(dataType string, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	case dataType == "NUMBER" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "NUMBER" && numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	default:
		return schema.Type{Name: dataType}
	}
}
