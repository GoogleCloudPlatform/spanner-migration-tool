export default interface IIssueReport {
    errors: Map<string, IssueDescription>
    warnings: Map<string, IssueDescription>
    suggestions: Map<string, IssueDescription>
    notes: Map<string, IssueDescription>
}

export interface IssueDescription {
    tableCount: number
    tableNames: Set<string>
}

export const TypeDescription = {
    MISSING_DEFAULT_VALUE_CONSTRAINTS: "Some columns have default values which HarbourBridge does not migrate. Please add the default constraints manually after the migration is complete",
    FOREIGN_KEY_USES: "Spanner does not support foreign keys",
    MISSING_PRIMARY_KEY: "Primary Key is missing",
    MULTI_DIMENSIONAL_ARRAY_USES: "Spanner doesn't support multi-dimensional arrays",
    INAPPROPIATE_TYPE: "No appropriate Spanner type",
    NUMERIC_USES: "Spanner does not support numeric. This type mapping could lose precision and is not recommended for production use",
    NUMERIC_THAT_FITS: "Spanner does not support numeric, but this type mapping preserves the numeric's specified precision",
    DECIMAL_USES: "Spanner does not support decimal. This type mapping could lose precision and is not recommended for production use",
    DECIMAL_THAT_FITS: "Spanner does not support decimal, but this type mapping preserves the decimal's specified precision",
    AUTOINCREMENTING_TYPE_USES: "Spanner does not support autoincrementing types",
    AUTO_INCREMENT_ATTRIBUTE_USES: "Spanner does not support auto_increment attribute",
    TIMESTAMP_SUGGESTION: "Spanner timestamp is closer to PostgreSQL timestamp",
    TIMESTAMP_WARNING: "Spanner timestamp is closer to MySQL timestamp",
    STORAGE_WARNING: "Some columns will consume more storage in Spanner",
    TIME_YEAR_TYPE_USES: "Spanner does not support time/year types",
    STRING_OVERFLOW_WARNING: "String overflow issue might occur as maximum supported length in Spanner is 2621440",
    TIMESTAMP_HOTSPOT: "Timestamp Hotspot Occured",
    AUTOINCREMENT_HOTSPOT: "Autoincrement Hotspot Occured",
    REDUNDANT_INDEX: "Redundant Index",
    AUTO_INCREMENT_INDEX: "Auto increment column in index can create a Hotspot",
    INTERLEAVE_INDEX_SUGGESTION: "Some columns can be interleaved",
    INTERLEAVED_NOT_IN_ORDER: "Some tables can be interleaved with parent table if primary key order parameter is changed to 1",
    INTERLEAVE_TABLE_SUGGESTION: "Some tables can be interleaved",
    ADD_INTERLEAVED_COLUMN: "If there is some primary key added in table, it can be interleaved",
    ILLEAGAL_NAME: "Names must adhere to the spanner regular expression {a-z|A-Z}[{a-z|A-Z|0-9|_}+]",
    RENAME_INTERLEAVED_COLUMN_PRIMARY_KEY: "If primary key is renamed in table to match the foreign key, the table can be interleaved",
    CHANGE_INTERLEAVED_COLUMN_SIZE: "If column size of this table's primary key is changed to match the foreign key, the table can be interleaved",
    ROW_LIMIT_EXCEEDED: "Non key columns exceed the spanner limit of 1600 MB. Please modify the column sizes",
    SHARD_ID_COLUMN_ADDED: "Column was added because this is a sharded migration and some column couldn't be dropped",
    SHARD_ID_ADD_COLUMN_PRIMARY_KEY: "Some column is not a part of primary key. Check for that column and add it as a part of Primary Key",
}