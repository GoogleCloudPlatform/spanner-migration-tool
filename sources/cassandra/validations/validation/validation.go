package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	spanner "github.com/googleapis/go-spanner-cassandra/cassandra/gocql"
)

var (
	sourceHost     = flag.String("source-host", "localhost", "Source Cassandra host")
	sourcePort     = flag.Int("source-port", 9042, "Source Cassandra port")
	sourceUsername = flag.String("source-username", "", "Source Cassandra username")
	sourcePassword = flag.String("source-password", "", "Source Cassandra password")
	spannerURI     = flag.String("spanner-uri", "", "Spanner database URI (projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID)")
	keyspace       = flag.String("keyspace", "", "Keyspace to validate")
	table          = flag.String("table", "", "Table to validate (mandatory)")
	batchSize      = flag.Int("batch-size", 100000, "Specifies how many rows to read and validate at a time. For sampling mode, it reads the specified number of rows during each sample.")
	workers        = flag.Int("workers", 1, "Number of parallel workers for validation")
	samplingMode   = flag.Bool("sampling-mode", false, "Validate a sample of rows instead of full matching")
	numSampleRows  = flag.Int("num-sample-rows", 0, "Number of rows to sample for validation (0 for indefinite sampling)")

	totalRowsProcessed        int
	totalMismatchesFound      int
	totalMissingFound         int
	totalErrorsDuringMatching int
	mu                        sync.Mutex
)

/*
This tool validates data consistency between source and target Cassandra clusters by comparing rows.
It supports two modes of operation:
1. Full validation mode (default): Validates all rows in the specified table
2. Sampling mode: Validates a random sample of rows, recommended for large datasets

Sample usage:

	go run validation.go \
	  --source-host localhost --source-port 9042 --source-username user1 --source-password pass1 \
	  --spanner-uri projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID \
	  --keyspace my_keyspace --table my_table \
	  --batch-size 100000 --workers 4

For sampling mode (recommended for large datasets):

	go run validation.go \
	  --source-host localhost --target-host remote-host \
	  --keyspace my_keyspace --table my_table \
	  --sampling-mode --num-sample-rows 1000000
*/
func main() {
	flag.Parse()

	if *table == "" {
		log.Fatal("The --table flag is mandatory.")
	}
	if *keyspace == "" {
		log.Fatal("The --keyspace flag is mandatory.")
	}
	if !*samplingMode && *numSampleRows != 0 {
		log.Fatal("The --sample-rows flag  can only be specified when --sampling-mode is true.")
	}

	sourceCluster := gocql.NewCluster(*sourceHost)
	sourceCluster.Port = *sourcePort
	sourceCluster.Keyspace = *keyspace
	if *sourceUsername != "" {
		sourceCluster.Authenticator = gocql.PasswordAuthenticator{Username: *sourceUsername, Password: *sourcePassword}
	}
	sourceSession, err := sourceCluster.CreateSession()
	if err != nil {
		log.Fatalf("Error creating source session: %v", err)
	}
	defer sourceSession.Close()

	opts := &spanner.Options{
		DatabaseUri: *spannerURI,
	}
	targetCluster := spanner.NewCluster(opts)
	// Important to close the adapter's resources
	defer spanner.CloseCluster(targetCluster)
	targetSession, err := targetCluster.CreateSession()
	if err != nil {
		log.Fatalf("Error creating target session: %v", err)
	}
	defer targetSession.Close()

	// TODO: Verify table exists on target once system query is fixed.
	err = verifyTableExists(sourceSession, *keyspace, *table)
	if err != nil {
		log.Fatalf("Source table '%s.%s' does not exist: %v", *keyspace, *table, err)
		os.Exit(1)
	}

	pkColumns, partitionKeyCount, err := getPrimaryKeyColumns(sourceSession)
	if err != nil {
		log.Fatalf("Error getting primary key columns: %v", err)
	}
	log.Printf("Primary Key Columns: %v", pkColumns)

	allColumns, err := getAllColumns(sourceSession)
	if err != nil {
		log.Fatalf("Error getting all columns: %v", err)
	}
	log.Printf("All Columns: %v", allColumns)

	allColumnsStr := strings.Join(allColumns, ", ")
	targetQuery := fmt.Sprintf("SELECT %s FROM %s.%s WHERE ", allColumnsStr, *keyspace, *table)

	var whereClauses []string

	for _, pkColumn := range pkColumns {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkColumn))
	}
	targetQuery += strings.Join(whereClauses, " AND ")
	log.Println("Target Query string: ", targetQuery)

	if !*samplingMode {
		validateEntireDataset(sourceSession, targetSession, targetQuery, pkColumns)
	} else {
		validateViaSampling(sourceSession, targetSession, targetQuery, pkColumns, partitionKeyCount)
	}

	log.Println("Validation complete.")
	log.Printf("Total rows processed: %d\n", totalRowsProcessed)
	log.Printf("Total errors during matching: %d\n", totalErrorsDuringMatching)
	log.Printf("Total mismatches found: %d\n", totalMismatchesFound)
	log.Printf("Total missing found: %d\n", totalMissingFound)
}

// MismatchDetail holds detail about a single mismatched row
type MismatchDetail struct {
	Key             map[string]interface{}
	SourceRow       map[string]interface{}
	TargetRow       map[string]interface{}
	MissingInTarget bool
}

func verifyTableExists(session *gocql.Session, keyspace string, table string) error {
	query := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?"
	var tableName string
	if err := session.Query(query, keyspace, table).Scan(&tableName); err != nil {
		if err == gocql.ErrNotFound {
			return fmt.Errorf("table %s does not exist in keyspace %s", table, keyspace)
		}
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	return nil
}

// getPrimaryKeyColumns retrieves both partition key and clustering columns for a table.
// It queries system tables to get column information and returns them in the correct order.
// Returns:
// - []string: ordered list of primary key columns (partition keys followed by clustering keys)
// - int: number of partition key columns
// - error: any error encountered during the process
func getPrimaryKeyColumns(session *gocql.Session) ([]string, int, error) {
	var pkColumns []string

	// Query to get partition key columns
	partitionKeyQuery := `
		SELECT column_name, position
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind = 'partition_key'
		ALLOW FILTERING
	`

	iter := session.Query(partitionKeyQuery, *keyspace, *table).Iter()
	partitionKeys := make(map[int]string)
	var columnName string
	var position int

	// Scan with position for later sorting
	for iter.Scan(&columnName, &position) {
		partitionKeys[position] = columnName
	}
	if err := iter.Close(); err != nil {
		return nil, 0, err
	}

	// Order partition keys by position
	for i := 0; i < len(partitionKeys); i++ {
		if key, ok := partitionKeys[i]; ok {
			pkColumns = append(pkColumns, key)
		}
	}

	// Query to get clustering columns
	clusteringKeyQuery := `
		SELECT column_name, position
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind = 'clustering'
		ALLOW FILTERING
	`

	iter = session.Query(clusteringKeyQuery, *keyspace, *table).Iter()
	clusteringKeys := make(map[int]string)

	// Scan with position for later sorting
	for iter.Scan(&columnName, &position) {
		clusteringKeys[position] = columnName
	}

	if err := iter.Close(); err != nil {
		return nil, 0, err
	}
	// Order clustering keys by position
	for i := 0; i < len(clusteringKeys); i++ {
		if key, ok := clusteringKeys[i]; ok {
			pkColumns = append(pkColumns, key)
		}
	}
	if len(pkColumns) == 0 {
		return nil, 0, fmt.Errorf("no primary key columns found, please verify keyspace name '%s' and table name '%s' are correct", *keyspace, *table)
	}

	return pkColumns, len(partitionKeys), nil
}

// getAllColumns retrieves all column names for the specified table.
// This includes both primary key and non-primary key columns.
// Returns:
// - []string: list of all column names in the table
// - error: any error encountered during the query
func getAllColumns(session *gocql.Session) ([]string, error) {
	var columns []string

	query := `
		SELECT column_name
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ?
	`

	iter := session.Query(query, *keyspace, *table).Iter()
	var columnName string
	for iter.Scan(&columnName) {
		columns = append(columns, columnName)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return columns, nil
}

// validateViaSampling performs data validation using random sampling of rows.
// It uses token-based sampling to randomly select rows from the source cluster
// and compares them with corresponding rows in the target cluster.
// This mode is recommended for large datasets where full validation is impractical.
func validateViaSampling(sourceSession *gocql.Session, targetSession *gocql.Session, targetQuery string, pkColumns []string, partitionKeyCount int) {
	// Build partition key columns string for token function, which does not expect the entire pk but only the partition key.
	partitionKeyStr := strings.Join(pkColumns[:partitionKeyCount], ", ")

	// Build sampling query with token
	samplingQuery := fmt.Sprintf("SELECT * FROM %s.%s WHERE token(%s) > ? LIMIT %d",
		*keyspace, *table, partitionKeyStr, *batchSize)
	log.Println("Sampling Query string: ", samplingQuery)

	for {
		// Generate random token value using time seed.
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		randToken := r.Int63()

		// Get batch of random rows
		iter := sourceSession.Query(samplingQuery, randToken).Iter()
		row := make(map[string]interface{})
		rows := make([]map[string]interface{}, 0)

		for iter.MapScan(row) {
			rows = append(rows, row)
			row = make(map[string]interface{})
		}

		if err := iter.Close(); err != nil {
			log.Fatalf("Error iterating through rows: %v", err)
		}

		if len(rows) == 0 {
			fmt.Println("No rows found...")
			continue
		}

		processBatch(rows, targetQuery, targetSession, pkColumns)

		// If numSampleRows is set and we've processed enough rows, stop
		if *numSampleRows != 0 && *&totalRowsProcessed >= *numSampleRows {
			break
		}
	}

}

// validateEntireDataset performs a full validation of all rows in the source table
// against the target table. It reads rows in batches to manage memory usage
// and processes them in parallel using multiple workers.
func validateEntireDataset(sourceSession *gocql.Session, targetSession *gocql.Session, targetQuery string, pkColumns []string) {
	sourceQuery := fmt.Sprintf("SELECT * FROM %s.%s", *keyspace, *table)
	iter := sourceSession.Query(sourceQuery).Iter()
	row := make(map[string]interface{})
	rows := make([]map[string]interface{}, 0)

	for iter.MapScan(row) {
		rows = append(rows, row)
		row = make(map[string]interface{})

		if len(rows) == *batchSize {
			processBatch(rows, targetQuery, targetSession, pkColumns)
			rows = make([]map[string]interface{}, 0)
		}
	}

	if len(rows) > 0 {
		processBatch(rows, targetQuery, targetSession, pkColumns)
	}

	if err := iter.Close(); err != nil {
		log.Fatalf("Error iterating through rows: %v", err)
	}
}

// processBatch handles the validation of a batch of rows and updates global statistics.
// It coordinates the parallel validation of rows and aggregates the results.
// Parameters:
// - rows: batch of rows to validate
// - targetQuery: prepared query string for fetching rows from target
// - targetSession: connection to target cluster
// - pkColumns: list of primary key columns used for row lookup
func processBatch(rows []map[string]interface{}, targetQuery string, targetSession *gocql.Session, pkColumns []string) {
	errors, mismatches, missing := validateRows(rows, targetQuery, targetSession, pkColumns)

	totalRowsProcessed += len(rows)
	totalMismatchesFound += mismatches
	totalMissingFound += missing
	totalErrorsDuringMatching += errors

	log.Printf("Processed: %d more rows, found %d errors, %d missing, %d mismatches\n",
		len(rows), errors, missing, mismatches)
	log.Printf("Total rows processed: %d, Total errors: %d, Total missing: %d, Total mismatches: %d\n",
		totalRowsProcessed, totalErrorsDuringMatching, totalMissingFound, totalMismatchesFound)
}

// validateRows performs parallel validation of source rows against target cluster.
// It distributes the work across multiple goroutines for better performance.
// Returns counts of errors, mismatches, and missing rows encountered during validation.
// Parameters:
// - sourceRows: rows from source cluster to validate
// - targetQuery: prepared query string for fetching rows from target
// - targetSession: connection to target cluster
// - pkColumns: list of primary key columns used for row lookup
func validateRows(sourceRows []map[string]interface{}, targetQuery string, targetSession *gocql.Session, pkColumns []string) (int, int, int) {
	errors := 0
	mismatches := 0
	missing := 0
	var wg sync.WaitGroup
	rowChan := make(chan map[string]interface{}, len(sourceRows))

	for _, row := range sourceRows {
		rowChan <- row
	}
	close(rowChan)

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sourceRow := range rowChan {
				var queryArgs []interface{}
				for _, col := range pkColumns {
					queryArgs = append(queryArgs, sourceRow[col])
				}
				targetRow := make(map[string]interface{})
				// TODO: Consider batched reads from Spanner instead of point reads per row.
				if err := targetSession.Query(targetQuery, queryArgs...).MapScan(targetRow); err != nil {
					if err == gocql.ErrNotFound {
						mu.Lock()
						missing++
						fmt.Printf("MISSING: row in target for source row: %+v\n", sourceRow)
						mu.Unlock()
					} else {
						fmt.Printf("ERROR: got error %v while reading target row for source: %+v\n", err, sourceRow)
						mu.Lock()
						errors++
						mu.Unlock()
					}
					continue
				}

				var mismatchDetails MismatchDetail
				diffFound, mismatchDetails := diffRows(sourceRow, targetRow)
				if !diffFound {
					continue
				}
				if mismatchDetails.MissingInTarget {
					mu.Lock()
					missing++
					fmt.Printf("MISSING: row in target for source row: %+v\n", sourceRow)
					mu.Unlock()
				} else {
					mu.Lock()
					mismatches++
					fmt.Printf("MISMATCH: found for row: %+v\n", mismatchDetails)
					mu.Unlock()

				}
			}
		}()
	}
	wg.Wait()
	return errors, mismatches, missing
}

// diffRows compares a row from the source cluster with its corresponding row in the target cluster.
// It checks for both missing rows and value mismatches in all columns.
// Returns:
// - bool: true if any difference is found
// - MismatchDetail: details of the mismatch if found
func diffRows(sourceRow, targetRow map[string]interface{}) (bool, MismatchDetail) {
	// If target row is nil, it's missing in target
	if targetRow == nil || len(targetRow) == 0 {
		return true, MismatchDetail{
			SourceRow:       sourceRow,
			TargetRow:       nil,
			MissingInTarget: true,
		}
	}

	// Compare all column values
	mismatch := false
	for col, sourceVal := range sourceRow {
		targetVal, exists := targetRow[col]
		if !exists || !compareValues(sourceVal, targetVal) {
			mismatch = true
			break
		}
	}

	if mismatch {
		return true, MismatchDetail{
			SourceRow:       sourceRow,
			TargetRow:       targetRow,
			MissingInTarget: false,
		}
	}

	return false, MismatchDetail{}
}

// compareValues performs deep comparison of two values that may be of different types.
// It handles special cases for various data types including:
// - nil values
// - byte arrays
// - maps
// - slices
// For other types, it falls back to string representation comparison.
// Returns true if the values are equal, false otherwise.
func compareValues(v1, v2 interface{}) bool {
	// Handle nil values
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	// Compare different types of values
	switch val1 := v1.(type) {
	case []byte:
		if val2, ok := v2.([]byte); ok {
			if len(val1) != len(val2) {
				return false
			}
			for i := range val1 {
				if val1[i] != val2[i] {
					return false
				}
			}
			return true
		}
	case map[string]interface{}:
		if val2, ok := v2.(map[string]interface{}); ok {
			if len(val1) != len(val2) {
				return false
			}
			for k, v := range val1 {
				if !compareValues(v, val2[k]) {
					return false
				}
			}
			return true
		}
	case []interface{}:
		if val2, ok := v2.([]interface{}); ok {
			if len(val1) != len(val2) {
				return false
			}
			for i := range val1 {
				if !compareValues(val1[i], val2[i]) {
					return false
				}
			}
			return true
		}
	}

	// For other types, use simple equality
	return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
}
