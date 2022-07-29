package updateTableSchema

/*
func canRemoveColumn(colName, table string) (int, error) {

	if isPartOfPK := isPartOfPK(colName, table); isPartOfPK {
		return http.StatusBadRequest, fmt.Errorf("column is part of primary key")
	}

	if isPartOfSecondaryIndex, _ := isPartOfSecondaryIndex(colName, table); isPartOfSecondaryIndex {
		return http.StatusPreconditionFailed, fmt.Errorf("column is part of secondary index, remove secondary index before making the update")
	}

	isPartOfFK := isPartOfFK(colName, table)

	isReferencedByFK, _ := isReferencedByFK(colName, table)

	if isPartOfFK || isReferencedByFK {
		return http.StatusPreconditionFailed, fmt.Errorf("column is part of foreign key relation, remove foreign key constraint before making the update")
	}

	return http.StatusOK, nil
}


func canRenameOrChangeType(colName, table string) (int, error) {

	sessionState := session.GetSessionState()

	isPartOfPK := utilities.IsPartOfPK(colName, table)

	isParent, childSchema := utilities.IsParent(table)

	isChild := sessionState.Conv.SpSchema[table].Parent != ""

	if isPartOfPK && (isParent || isChild) {
		return http.StatusBadRequest, fmt.Errorf("column : '%s' in table : '%s' is part of parent-child relation with schema : '%s'", colName, table, childSchema)
	}

	if isPartOfSecondaryIndex, indexName := isPartOfSecondaryIndex(colName, table); isPartOfSecondaryIndex {
		return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of secondary index : '%s', remove secondary index before making the update",
			colName, table, indexName)
	}

	isPartOfFK := isPartOfFK(colName, table)

	isReferencedByFK, relationTable := isReferencedByFK(colName, table)

	if isPartOfFK || isReferencedByFK {
		if isReferencedByFK {
			return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of foreign key relation with table : '%s', remove foreign key constraint before making the update",
				colName, table, relationTable)
		}
		return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of foreign keys, remove foreign key constraint before making the update",
			colName, table)
	}

	return http.StatusOK, nil
}



		_, ok := Conv.SpSchema[table].ColDefs[colName]

		fmt.Println("ok :", ok)

		if !ok {

			log.Println("colname not found in table")
			http.Error(w, fmt.Sprintf("colname not found in table"), http.StatusBadRequest)
			return

		}

*/
