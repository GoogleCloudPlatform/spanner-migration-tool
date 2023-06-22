// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	_ "github.com/lib/pq" // we will use database/sql package instead of using this package directly
	"google.golang.org/api/iterator"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// InfoSchemaImpl postgres specific implementation for InfoSchema.
type InfoSchemaImpl struct {
	Client    *spanner.Client
	Ctx       context.Context
	SpDialect string
}

// GetToDdl function below implement the common.InfoSchema interface.
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// We leave the 5 functions below empty to be able to pass this as an infoSchema interface. We don't need these for now.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, tableId string, srcSchema schema.Table, spCols []string, spSchema ddl.CreateTable, additionalAttributes internal.AdditionalDataAttributes) error {
	return nil
}

// GetRowCount returns the row count of the table.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	q := "SELECT count(*) FROM " + table.Name + ";"
	stmt := spanner.Statement{
		SQL: q,
	}
	iter := isi.Client.Single().Query(isi.Ctx, stmt)
	defer iter.Stop()
	var count int64
	row, err := iter.Next()
	if err == iterator.Done {
		return 0, nil
	}
	if err != nil {
		return count, err
	}
	row.Columns(&count)
	return count, err

}

func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error) {
	return nil, nil
}

func (isi InfoSchemaImpl) StartChangeDataCapture(ctx context.Context, conv *internal.Conv) (map[string]interface{}, error) {
	return nil, nil
}

func (isi InfoSchemaImpl) StartStreamingMigration(ctx context.Context, client *spanner.Client, conv *internal.Conv, streamingInfo map[string]interface{}) error {
	return nil
}

// GetTableName returns table name.
func (isi InfoSchemaImpl) GetTableName(schema string, tableName string) string {
	if isi.SpDialect == constants.DIALECT_POSTGRESQL {
		if schema == "public" { // Drop public prefix for pg spanner.
			return tableName
		}
	} else {
		if schema == "" {
			return tableName
		}
	}
	return fmt.Sprintf("%s.%s", schema, tableName)
}

// GetTables return list of tables in the selected database.
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	q := `SELECT table_schema, table_name FROM information_schema.tables 
	WHERE table_type = 'BASE TABLE' AND table_schema = ''`
	if isi.SpDialect == constants.DIALECT_POSTGRESQL {
		q = `SELECT table_schema, table_name FROM information_schema.tables 
	WHERE table_type = 'BASE TABLE' AND table_schema = 'public'`
	}
	stmt := spanner.Statement{SQL: q}
	iter := isi.Client.Single().Query(isi.Ctx, stmt)
	defer iter.Stop()

	var tableSchema, tableName string
	var tables []common.SchemaAndName
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("couldn't get tables: %w", err)
		}
		err = row.Columns(&tableSchema, &tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, common.SchemaAndName{Schema: tableSchema, Name: tableName})
	}
	return tables, nil
}

// GetColumns returns a list of Column objects and names
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	q := `SELECT column_name, spanner_type, is_nullable 
			FROM information_schema.columns
			WHERE table_schema = '' AND table_name = @p1
			ORDER BY ordinal_position;`
	if isi.SpDialect == constants.DIALECT_POSTGRESQL {
		q = `SELECT column_name, spanner_type, is_nullable 
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1
			ORDER BY ordinal_position;`
	}
	stmt := spanner.Statement{
		SQL: q,
		Params: map[string]interface{}{
			"p1": table.Name,
		},
	}
	iter := isi.Client.Single().Query(isi.Ctx, stmt)
	defer iter.Stop()

	colDefs := make(map[string]schema.Column)
	var colIds []string
	var colName, spannerType, isNullable string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("couldn't get column info for table %s: %s", table.Name, err)
		}
		err = row.Columns(&colName, &spannerType, &isNullable)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot read row for table %s while reading columns: %s", table.Name, err)
		}
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
			switch c {
			case "CHECK":
				ignored.Check = true
			case "FOREIGN KEY", "PRIMARY KEY", "UNIQUE":
				// Nothing to do here -- these are handled elsewhere.
			}
		}
		colId := internal.GenerateColumnId()
		c := schema.Column{
			Id:      colId,
			Name:    colName,
			Type:    toType(spannerType),
			NotNull: common.ToNotNull(conv, isNullable),
		}
		colDefs[colId] = c
		colIds = append(colIds, colId)
	}
	return colDefs, colIds, nil
}

// GetConstraints returns a list of primary keys and by-column map of
// other constraints.  Note: we need to preserve ordinal order of
// columns in primary key constraints.
// Note that foreign key constraints are handled in getForeignKeys.
func (isi InfoSchemaImpl) GetConstraints(conv *internal.Conv, table common.SchemaAndName) ([]string, map[string][]string, error) {
	q := `SELECT k.column_name, t.constraint_type
              FROM information_schema.table_constraints AS t
                INNER JOIN information_schema.KEY_COLUMN_USAGE AS k
                  ON t.constraint_name = k.constraint_name AND t.constraint_schema = k.constraint_schema
              WHERE k.table_schema = '' AND k.table_name = @p1 ORDER BY k.ordinal_position;`
	if isi.SpDialect == constants.DIALECT_POSTGRESQL {
		q = `SELECT k.column_name, t.constraint_type
		FROM information_schema.table_constraints AS t
		  INNER JOIN information_schema.KEY_COLUMN_USAGE AS k
			ON t.constraint_name = k.constraint_name AND t.constraint_schema = k.constraint_schema
		WHERE k.table_schema = 'public' AND k.table_name = $1 ORDER BY k.ordinal_position;`
	}
	stmt := spanner.Statement{
		SQL: q,
		Params: map[string]interface{}{
			"p1": table.Name,
		},
	}
	iter := isi.Client.Single().Query(isi.Ctx, stmt)
	defer iter.Stop()

	var primaryKeys []string
	var col, constraint string
	m := make(map[string][]string)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("couldn't get row while reading constraints: %w", err)
		}
		err = row.Columns(&col, &constraint)
		if err != nil {
			return nil, nil, err
		}
		if col == "" || constraint == "" {
			conv.Unexpected("Got empty col or constraint")
			continue
		}
		switch constraint {
		case "PRIMARY KEY":
			primaryKeys = append(primaryKeys, col)
		default:
			m[col] = append(m[col], constraint)
		}
	}
	return primaryKeys, m, nil
}

// GetForeignKeys returns a list of all the foreign key constraints.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	q := `SELECT  k.constraint_name, k.column_name, c.table_name, c.column_name 
			FROM information_schema.key_column_usage AS k 
			JOIN information_schema.constraint_column_usage AS c ON k.constraint_name = c.constraint_name
			JOIN information_schema.table_constraints AS t ON k.constraint_name = t.constraint_name 
			WHERE t.constraint_type='FOREIGN KEY' AND t.table_schema = '' AND t.table_name = @p1
			ORDER BY k.constraint_name, k.ordinal_position;`
	if isi.SpDialect == constants.DIALECT_POSTGRESQL {
		q = `SELECT  k.constraint_name, k.column_name, c.table_name, c.column_name 
				FROM information_schema.key_column_usage AS k 
				JOIN information_schema.constraint_column_usage AS c ON k.constraint_name = c.constraint_name
				JOIN information_schema.table_constraints AS t ON k.constraint_name = t.constraint_name 
				WHERE t.constraint_type='FOREIGN KEY' AND t.table_schema = 'public' AND t.table_name = $1
				ORDER BY k.constraint_name, k.ordinal_position;`
	}
	stmt := spanner.Statement{
		SQL: q,
		Params: map[string]interface{}{
			"p1": table.Name,
		},
	}
	iter := isi.Client.Single().Query(isi.Ctx, stmt)
	defer iter.Stop()

	var col, refCol, fKeyName, refTable string
	fKeys := make(map[string]common.FkConstraint)
	var keyNames []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("couldn't get row while fetching foreign keys: %w", err)
		}
		err = row.Columns(&fKeyName, &col, &refTable, &refCol)
		if err != nil {
			return nil, err
		}
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.Cols = append(fk.Cols, col)
			fk.Refcols = append(fk.Refcols, refCol)
			fKeys[fKeyName] = fk
			continue
		}
		fKeys[fKeyName] = common.FkConstraint{Name: fKeyName, Table: isi.GetTableName(table.Schema, refTable), Refcols: []string{refCol}, Cols: []string{col}}
		keyNames = append(keyNames, fKeyName)
	}
	sort.Strings(keyNames)
	for _, k := range keyNames {
		// The query returns a crypted result for multi-col FKs. Currently for a FK from (a,b,c) -> (x,y,z),
		// the returned rows like (a,x), (a,y), (a,z), (b,x), (b,y), (b,z), (c,x), (c,y), (c,z).
		// Need to reduce it to (a,x), (b,y), (c,z). The logic below does that.
		n := int(math.Sqrt(float64(len(fKeys[k].Cols))))
		cols, refcols := []string{}, []string{}
		for i := 0; i < n; i++ {
			cols = append(cols, fKeys[k].Cols[i*n])
			refcols = append(refcols, fKeys[k].Refcols[i])
		}
		foreignKeys = append(foreignKeys,
			schema.ForeignKey{
				Id:               internal.GenerateForeignkeyId(),
				Name:             fKeys[k].Name,
				ColumnNames:      cols,
				ReferTableName:   fKeys[k].Table,
				ReferColumnNames: refcols})
	}
	return foreignKeys, nil
}

// GetIndexes returns a list of Indexes per table.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName, colNameIdMap map[string]string) ([]schema.Index, error) {
	q := `SELECT distinct c.INDEX_NAME,c.COLUMN_NAME,c.ORDINAL_POSITION,c.COLUMN_ORDERING,i.IS_UNIQUE
			FROM information_schema.index_columns AS c
			JOIN information_schema.indexes AS i
			ON c.INDEX_NAME=i.INDEX_NAME
			WHERE c.table_schema = '' AND i.INDEX_TYPE='INDEX' AND c.TABLE_NAME = @p1 ORDER BY c.INDEX_NAME, c.ORDINAL_POSITION;`
	if isi.SpDialect == constants.DIALECT_POSTGRESQL {
		q = `SELECT distinct c.INDEX_NAME,c.COLUMN_NAME,c.ORDINAL_POSITION,c.COLUMN_ORDERING,i.IS_UNIQUE
		FROM information_schema.index_columns AS c
		JOIN information_schema.indexes AS i
		ON c.INDEX_NAME=i.INDEX_NAME
		WHERE c.table_schema = 'public' AND i.INDEX_TYPE='INDEX' AND c.TABLE_NAME = $1 ORDER BY c.INDEX_NAME, c.ORDINAL_POSITION;`
	}
	stmt := spanner.Statement{
		SQL: q,
		Params: map[string]interface{}{
			"p1": table.Name,
		},
	}
	iter := isi.Client.Single().Query(isi.Ctx, stmt)
	defer iter.Stop()
	var name, column, ordering string
	var isUnique bool
	var sequence int64
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	var indexes []schema.Index
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("couldn't read row while fetching interleaved tables: %w", err)
		}
		err = row.Columns(&name, &column, &sequence, &ordering, &isUnique)
		if err != nil {
			fmt.Println(err)
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{
				Id:     internal.GenerateIndexesId(),
				Name:   name,
				Unique: isUnique}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{
			ColId: colNameIdMap[column],
			Desc:  (ordering == "DESC")})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		indexes = append(indexes, indexMap[k])
	}
	return indexes, nil
}

func (isi InfoSchemaImpl) GetInterleaveTables() (map[string]string, error) {
	q := `SELECT table_name, parent_table_name FROM information_schema.tables 
	WHERE interleave_type = 'IN PARENT' AND table_type = 'BASE TABLE' AND table_schema = ''`
	if isi.SpDialect == constants.DIALECT_POSTGRESQL {
		q = `SELECT table_name, parent_table_name FROM information_schema.tables 
		WHERE interleave_type = 'IN PARENT' AND table_type = 'BASE TABLE' AND table_schema = 'public'`
	}
	stmt := spanner.Statement{SQL: q}
	iter := isi.Client.Single().Query(isi.Ctx, stmt)
	defer iter.Stop()

	var tableName, parentTable string
	parentTables := map[string]string{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("couldn't read row while fetching interleaved tables: %w", err)
		}
		err = row.Columns(&tableName, &parentTable)
		if err != nil {
			return nil, err
		}
		parentTables[tableName] = parentTable
	}
	return parentTables, nil
}

func toType(dataType string) schema.Type {
	switch {
	case strings.Contains(dataType, "ARRAY"):
		typeLenStr := dataType[(strings.Index(dataType, "<") + 1):(len(dataType) - 1)]
		schemaType := toType(typeLenStr)
		schemaType.ArrayBounds = []int64{-1}
		return schemaType
	case strings.Contains(dataType, "("):
		idx := strings.Index(dataType, "(")
		typeLenStr := dataType[(idx + 1):(len(dataType) - 1)]
		var typeLen int64
		if typeLenStr == "MAX" {
			typeLen = ddl.MaxLength
		} else {
			typeLen, _ = strconv.ParseInt(typeLenStr, 10, 64)
		}
		return schema.Type{Name: dataType[:idx], Mods: []int64{typeLen}}
	default:
		return schema.Type{Name: dataType}
	}
}
