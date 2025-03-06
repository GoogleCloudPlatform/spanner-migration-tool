// Copyright 2025 Google LLC
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

package assessment

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
)

func PrintColumnDef(col schema.Column) string {
	var columnDef strings.Builder

	columnDef.WriteString(quote(col.Name))
	columnDef.WriteString(" ")
	columnDef.WriteString(col.Type.Name)

	if len(col.Type.Mods) > 0 {
		columnDef.WriteString("(")
		for i, mod := range col.Type.Mods {
			columnDef.WriteString(strconv.FormatInt(mod, 10))
			if i < len(col.Type.Mods)-1 {
				columnDef.WriteString(", ")
			}
		}
		columnDef.WriteString(")")
	}

	if len(col.Type.ArrayBounds) > 0 {
		for _, bound := range col.Type.ArrayBounds {
			columnDef.WriteString(fmt.Sprintf("[%d]", bound))
		}
	}

	if col.NotNull {
		columnDef.WriteString(" NOT NULL")
	}

	if col.AutoGen.Name != "" && col.AutoGen.GenerationType == constants.AUTO_INCREMENT {
		columnDef.WriteString(" AUTO_INCREMENT") // Basic auto increment, adjust for others as needed.
	}

	if col.DefaultValue.IsPresent {
		columnDef.WriteString(" DEFAULT ")
		columnDef.WriteString(col.DefaultValue.Value.Statement)
	}

	return columnDef.String()

}

// PrintCreateTable unparses a CREATE TABLE statement.
func PrintCreateTable(ct schema.Table) string {
	var col []string
	var keys []string
	for _, colId := range ct.ColIds {
		s := PrintColumnDef(ct.ColDefs[colId])
		col = append(col, s)
	}

	orderedPks := []schema.Key{}
	orderedPks = append(orderedPks, ct.PrimaryKeys...)
	sort.Slice(orderedPks, func(i, j int) bool {
		return orderedPks[i].Order < orderedPks[j].Order
	})

	for _, key := range orderedPks {
		colName := quote(ct.ColDefs[key.ColId].Name)
		if key.Desc {
			colName = colName + " DESC"
		}
		keys = append(keys, colName)
	}

	var checkString string
	if len(ct.CheckConstraints) > 0 {
		checkString = FormatCheckConstraints(ct.CheckConstraints)
	} else {
		checkString = ""
	}

	if len(keys) == 0 {
		return fmt.Sprintf("CREATE TABLE %s (\n%s%s);", quote(ct.Name), strings.Join(col, ", "), checkString)
	}
	return fmt.Sprintf("CREATE TABLE %s (\n%s%s, PRIMARY KEY (%s));", quote(ct.Name), strings.Join(col, ", "), checkString, strings.Join(keys, ", "))
}

// PrintCreateIndex unparses a CREATE INDEX statement.
func PrintCreateIndex(index schema.Index, ct schema.Table) string {
	var createIndex strings.Builder

	createIndex.WriteString("CREATE ")
	if index.Unique {
		createIndex.WriteString("UNIQUE ")
	}
	createIndex.WriteString("INDEX ")
	createIndex.WriteString(index.Name)
	createIndex.WriteString(" ON ")
	createIndex.WriteString(ct.Name)
	createIndex.WriteString(" (")

	// Sort keys by order
	sort.Slice(index.Keys, func(i, j int) bool {
		return index.Keys[i].Order < index.Keys[j].Order
	})

	for i, key := range index.Keys {
		colName := ct.ColDefs[key.ColId].Name
		createIndex.WriteString(colName)
		if key.Desc {
			createIndex.WriteString(" DESC")
		}
		if i < len(index.Keys)-1 {
			createIndex.WriteString(", ")
		}
	}

	createIndex.WriteString(");")
	createIndex.WriteString(";")

	return createIndex.String()
}

// PrintForeignKeyAlterTable unparses the foreign keys using ALTER TABLE.
func PrintForeignKeyAlterTable(fk schema.ForeignKey, tableId string, srcSchema map[string]schema.Table) string {
	var alterTable strings.Builder
	tableName := srcSchema[tableId].Name

	alterTable.WriteString(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT ", quote(tableName)))

	if fk.Name != "" {
		alterTable.WriteString(fmt.Sprintf("%s ", quote(fk.Name)))
	}

	alterTable.WriteString("FOREIGN KEY (")

	// Add columns in the current table
	for i, colId := range fk.ColIds {
		alterTable.WriteString(fmt.Sprintf("%s", quote(srcSchema[tableId].ColDefs[colId].Name)))
		if i < len(fk.ColIds)-1 {
			alterTable.WriteString(", ")
		}
	}

	alterTable.WriteString(fmt.Sprintf(") REFERENCES %s (", quote(srcSchema[fk.ReferTableId].Name)))

	// Add referenced columns
	for i, refColId := range fk.ReferColumnIds {
		refColName := srcSchema[fk.ReferTableId].ColDefs[refColId].Name
		alterTable.WriteString(fmt.Sprintf("%s", quote(refColName)))
		if i < len(fk.ReferColumnIds)-1 {
			alterTable.WriteString(", ")
		}
	}

	alterTable.WriteString(")")

	// Add ON DELETE and ON UPDATE actions
	if fk.OnDelete != "" {
		alterTable.WriteString(fmt.Sprintf(" ON DELETE %s", strings.ToUpper(fk.OnDelete)))
	}
	if fk.OnUpdate != "" {
		alterTable.WriteString(fmt.Sprintf(" ON UPDATE %s", strings.ToUpper(fk.OnUpdate)))
	}

	alterTable.WriteString(";")
	return alterTable.String()
}

// FormatCheckConstraints formats the check constraints in SQL syntax.
func FormatCheckConstraints(cks []schema.CheckConstraint) string {
	var builder strings.Builder

	for _, col := range cks {
		if col.Name != "" {
			builder.WriteString(fmt.Sprintf(", CONSTRAINT %s CHECK (%s)", quote(col.Name), col.Expr))
		} else {
			builder.WriteString(fmt.Sprintf(", CHECK (%s)", col.Expr))
		}
	}

	return builder.String()
}

// GetDDL returns the string representation of MySQL schema represented by schema.Table struct.
func GetDDL(tableSchema map[string]schema.Table) string {
	var ddl []string

	for tableId := range tableSchema {
		ddl = append(ddl, PrintCreateTable(tableSchema[tableId]))
		for _, index := range tableSchema[tableId].Indexes {
			ddl = append(ddl, PrintCreateIndex(index, tableSchema[tableId]))
		}
	}
	// Append foreign key constraints to DDL.
	for t := range tableSchema {
		for _, fk := range tableSchema[t].ForeignKeys {
			ddl = append(ddl, PrintForeignKeyAlterTable(fk, tableSchema[t].Id, tableSchema))
		}
	}

	return strings.Join(ddl, "\n\n")
}

func quote(name string) string {
	name = "`" + name + "`"
	return name
}
