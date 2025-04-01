package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/gocql/gocql"
	"google.golang.org/api/iterator"
)

// This script connects to both Cassandra and Spanner databases to count the number of rows in specified tables.
// It allows for parallel processing of the counting operation using multiple workers.
//
// Sample usage:
// go run count.go -host localhost -port 9042 -keyspace my_keyspace -spanner-uri projects/my_project/instances/my_instance/databases/my_database
func main() {
	// Define command line arguments
	host := flag.String("host", "localhost", "Cassandra host")
	port := flag.Int("port", 9042, "Cassandra port")
	keyspace := flag.String("keyspace", "", "Keyspace name")
	username := flag.String("username", "", "Cassandra username")
	password := flag.String("password", "", "Cassandra password")
	table := flag.String("table", "", "Table name (empty for all tables, or comma-separated list of tables)")
	workers := flag.Int("workers", 8, "Number of parallel workers")
	spannerURI := flag.String("spanner-uri", "", "Spanner database URI (projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID)")
	flag.Parse()

	if *keyspace == "" {
		fmt.Println("keyspace name is required")
		os.Exit(1)
	}

	if *spannerURI == "" {
		fmt.Println("spanner-uri is required")
		os.Exit(1)
	}

	// Connect to Cassandra
	cassSession, err := connectToCassandra(*host, *port, *keyspace, *username, *password)
	if err != nil {
		fmt.Printf("Failed to connect to Cassandra: %v\n", err)
		os.Exit(1)
	}
	defer cassSession.Close()

	// Connect to Spanner
	ctx := context.Background()
	spannerClient, err := connectToSpanner(ctx, *spannerURI)
	if err != nil {
		fmt.Printf("Failed to connect to Spanner: %v\n", err)
		os.Exit(1)
	}
	defer spannerClient.Close()

	tables := []string{}
	if *table != "" {
		tables = strings.Split(*table, ",")
		for i, t := range tables {
			tables[i] = strings.TrimSpace(t)
		}
	} else {
		cassandraTables, err := getCassandraTables(cassSession, *keyspace)
		if err != nil {
			fmt.Printf("Error fetching Cassandra tables: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Found %d tables in Cassandra: %v\n", len(cassandraTables), cassandraTables)

		spannerTables, err := getSpannerTables(ctx, spannerClient)
		if err != nil {
			fmt.Printf("Error fetching Spanner tables: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Found %d tables in Spanner: %v\n", len(spannerTables), spannerTables)

		spannerTableSet := make(map[string]struct{}, len(spannerTables))
		for _, t := range spannerTables {
			spannerTableSet[t] = struct{}{}
		}

		for _, cassTable := range cassandraTables {
			if _, found := spannerTableSet[cassTable]; found {
				tables = append(tables, cassTable)
			}
		}

		if len(tables) == 0 {
			fmt.Println("No common tables found between Cassandra and Spanner.")
			os.Exit(0)
		}
	}

	fmt.Printf("Getting counts for tables: %v\n", tables)

	// Get token ranges for the cluster
	// TODO: Consider generating custom token ranges based on size estimates instead of relying on cassandra partitions.
	tokenRanges, err := getClusterTokenRanges(cassSession)
	if err != nil {
		fmt.Printf("Error fetching token ranges: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Found %d token ranges across the cluster\n", len(tokenRanges))

	// TODO: Consider parallelizing across tables.
	for _, tableName := range tables {
		fmt.Printf("\nTable: %s\n", tableName)
		fmt.Printf("----------------------------------------\n")

		result := countBothDatabases(ctx, cassSession, spannerClient, *keyspace, tableName, tokenRanges, *workers)

		// Print Cassandra results
		if result.CassandraError != nil {
			fmt.Printf("  Cassandra count: ERROR - %v\n", result.CassandraError)
		} else {
			fmt.Printf("  Cassandra count: %d\n", result.CassandraCount)
		}

		// Print Spanner results
		if result.SpannerError != nil {
			fmt.Printf("  Spanner count:   ERROR - %v\n", result.SpannerError)
		} else {
			fmt.Printf("  Spanner count:   %d\n", result.SpannerCount)
		}
		fmt.Printf("----------------------------------------\n")
	}
}

// getCassandraTables fetches all user table names from the specified keyspace in Cassandra.
func getCassandraTables(session *gocql.Session, keyspace string) ([]string, error) {
	var tables []string
	iter := session.Query(`SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?`, keyspace).Iter()
	var tableName string
	for iter.Scan(&tableName) {
		tables = append(tables, tableName)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("querying system_schema.tables failed: %w", err)
	}
	return tables, nil
}

// getSpannerTables fetches all user table names from the connected Spanner database.
func getSpannerTables(ctx context.Context, client *spanner.Client) ([]string, error) {
	var tables []string
	stmt := spanner.Statement{SQL: `SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_catalog = '' AND table_schema = ''`}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("querying INFORMATION_SCHEMA.TABLES failed: %w", err)
		}
		var tableName string
		if err := row.Columns(&tableName); err != nil {
			return nil, fmt.Errorf("reading table name from Spanner result failed: %w", err)
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

// TokenRange represents a Cassandra token range
type TokenRange struct {
	Start string
	End   string
}

// TableCount holds row counts for both Cassandra and Spanner tables
type TableCount struct {
	TableName      string
	CassandraCount int64
	SpannerCount   int64
	CassandraError error
	SpannerError   error
}

// connectToCassandra establishes a connection to the Cassandra cluster
func connectToCassandra(host string, port int, keyspace, username, password string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(host)
	cluster.Port = port
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 30 * time.Second

	// Add authentication if credentials are provided
	if username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	return cluster.CreateSession()
}

// connectToSpanner establishes a connection to the Spanner database
func connectToSpanner(ctx context.Context, uri string) (*spanner.Client, error) {
	return spanner.NewClient(ctx, uri)
}

// countSpannerRows counts the total number of rows in a Spanner table
func countSpannerRows(ctx context.Context, client *spanner.Client, table string) (int64, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT COUNT(*) FROM %s", table),
	}

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return 0, fmt.Errorf("no results returned from count query")
	}
	if err != nil {
		return 0, fmt.Errorf("error executing count query: %w", err)
	}

	var count int64
	if err := row.Columns(&count); err != nil {
		return 0, fmt.Errorf("error scanning count result: %w", err)
	}

	return count, nil
}

// getClusterTokenRanges retrieves and processes token ranges from all nodes in the Cassandra cluster.
// It combines tokens from both local and peer nodes, deduplicates them, and creates a sorted list of
// token ranges that cover the entire token ring.
// TODO: Consider creating configurable number of partitions instead of system ranges.
func getClusterTokenRanges(session *gocql.Session) ([]TokenRange, error) {
	localTokens, err := getLocalNodeTokens(session)
	if err != nil {
		return nil, fmt.Errorf("failed to get local node tokens: %w", err)
	}

	peerTokens, err := getPeerNodeTokens(session)
	if err != nil {
		return nil, fmt.Errorf("failed to get peer node tokens: %w", err)
	}

	// 1. Combine and Deduplicate Tokens
	allTokens := append(localTokens, peerTokens...)
	uniqueTokens := distinct(allTokens)

	// 2. Map to Big Ints and Sort
	bigIntTokens := make([]*big.Int, len(uniqueTokens))
	for i, tokenStr := range uniqueTokens {
		bigIntToken := new(big.Int)
		if _, ok := bigIntToken.SetString(tokenStr, 10); !ok {
			return nil, fmt.Errorf("invalid token string: %s", tokenStr)
		}
		bigIntTokens[i] = bigIntToken
	}
	sort.Slice(bigIntTokens, func(i, j int) bool {
		return bigIntTokens[i].Cmp(bigIntTokens[j]) < 0
	})

	// 3. Create Token Ranges
	tokenRanges := make([]TokenRange, len(bigIntTokens)+1)
	// First range
	tokenRanges[0] = TokenRange{Start: "nil", End: bigIntTokens[0].String()}

	// Middle ranges
	for i := 1; i < len(bigIntTokens); i++ {
		tokenRanges[i].Start = bigIntTokens[i-1].String()
		tokenRanges[i].End = bigIntTokens[i].String()

	}

	// Last range
	lastIndex := len(tokenRanges) - 1
	tokenRanges[lastIndex] = TokenRange{Start: bigIntTokens[lastIndex-1].String(), End: "nil"}

	return tokenRanges, nil
}

// distinct takes a slice of strings and returns a new slice containing only unique elements,
// removing any duplicates from the input slice.
func distinct(tokens []string) []string {
	seen := make(map[string]bool)
	unique := []string{}
	for _, token := range tokens {
		if _, ok := seen[token]; !ok {
			seen[token] = true
			unique = append(unique, token)
		}
	}
	return unique
}

// getLocalNodeTokens retrieves the token ranges assigned to the local Cassandra node.
// It queries the system.local table to get the tokens for the current node.
func getLocalNodeTokens(session *gocql.Session) ([]string, error) {
	var tokens []string // Corrected: Should be a slice of strings
	err := session.Query("SELECT tokens FROM system.local").Scan(&tokens)
	if err != nil {
		return nil, err
	}

	// No need to trim or split; the driver handles this for slices
	return tokens, nil
}

// getPeerNodeTokens retrieves the token ranges assigned to all peer nodes in the Cassandra cluster.
// It queries the system.peers_v2 table to get tokens for all other nodes in the cluster.
func getPeerNodeTokens(session *gocql.Session) ([]string, error) {
	var allTokens []string
	iter := session.Query("SELECT tokens FROM system.peers_v2").Iter() // Use peers_v2 for modern Cassandra
	var tokens []string                                                // Corrected: Scan into a slice

	for iter.Scan(&tokens) {
		allTokens = append(allTokens, tokens...) // Append directly; no splitting needed
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return allTokens, nil
}

// getPartitionKeysFromMetadata retrieves the partition key columns for a given table from Cassandra's metadata.
// It returns the partition key column names in the correct order as defined in the table schema.
func getPartitionKeysFromMetadata(session *gocql.Session, keyspaceName, tableName string) ([]string, error) {
	// Get keyspace metadata which contains table information
	keyspaceMetadata, err := session.KeyspaceMetadata(keyspaceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get keyspace metadata: %w", err)
	}

	// Get table metadata
	table, ok := keyspaceMetadata.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found in keyspace %s", tableName, keyspaceName)
	}

	// Get partition key columns in correct order
	var partitionKeyColumns []string
	for _, column := range table.PartitionKey {
		partitionKeyColumns = append(partitionKeyColumns, column.Name)
	}

	if len(partitionKeyColumns) == 0 {
		return nil, fmt.Errorf("no partition keys found for table %s", tableName)
	}

	return partitionKeyColumns, nil
}

// countTableRows counts rows in both Cassandra and Spanner concurrently
func countBothDatabases(ctx context.Context, cassSession *gocql.Session, spannerClient *spanner.Client,
	keyspace, tableName string, tokenRanges []TokenRange, workers int) TableCount {
	result := TableCount{TableName: tableName}
	var wg sync.WaitGroup
	wg.Add(2)

	// Count Cassandra rows in parallel
	go func() {
		defer wg.Done()
		count, err := countCassandraRows(cassSession, keyspace, tableName, tokenRanges, workers)
		result.CassandraCount = count
		result.CassandraError = err
	}()

	// Count Spanner rows in parallel
	go func() {
		defer wg.Done()
		count, err := countSpannerRows(ctx, spannerClient, tableName)
		result.SpannerCount = count
		result.SpannerError = err
	}()

	wg.Wait()
	return result
}

// countCassandraRows counts the total number of rows in a Cassandra table by getting partition keys
// and using token ranges for parallel counting.
func countCassandraRows(session *gocql.Session, keyspace, tableName string, tokenRanges []TokenRange, workers int) (int64, error) {
	// Get table metadata including partition keys
	partitionKeys, err := getPartitionKeysFromMetadata(session, keyspace, tableName)
	if err != nil {
		return 0, fmt.Errorf("error getting partition keys: %w", err)
	}

	// Count rows using token ranges
	return countTableRows(session, keyspace, tableName, partitionKeys, tokenRanges, workers)
}

// countTableRows counts the total number of rows in a table by dividing the work across multiple token ranges
// and processing them in parallel using a worker pool. It aggregates the results from all workers and handles
// any errors that occur during the counting process.
func countTableRows(session *gocql.Session, keyspace, table string, partitionKeys []string,
	tokenRanges []TokenRange, workers int) (int64, error) {
	// Create channels for work distribution and results
	workChan := make(chan TokenRange, len(tokenRanges))
	resultChan := make(chan int64, len(tokenRanges))
	errChan := make(chan error, workers)

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tr := range workChan {
				count, err := countInRange(session, keyspace, table, partitionKeys, tr)
				if err != nil {
					errChan <- err
					return
				}
				resultChan <- count
			}
		}()
	}

	// Send token ranges to worker pool
	for _, tr := range tokenRanges {
		workChan <- tr
	}
	close(workChan)

	// Wait for all workers to finish and close result channel
	go func() {
		wg.Wait()
		close(resultChan)
		close(errChan)
	}()

	// Collect results and check for errors
	var totalCount int64
	for count := range resultChan {
		totalCount += count
	}

	// Check for errors
	for err := range errChan {
		if err != nil {
			return 0, err // Return the *first* error encountered
		}
	}

	return totalCount, nil
}

// countInRange counts the number of rows within a specific token range for a given table.
// It constructs a query using the TOKEN function to filter rows based on the partition key's token value
// falling within the specified range.
func countInRange(session *gocql.Session, keyspace, table string, partitionKeys []string, tr TokenRange) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Build partition key string for TOKEN function
	partitionKeyStr := partitionKeys[0]
	if len(partitionKeys) > 1 {
		partitionKeyStr = fmt.Sprintf("%s", strings.Join(partitionKeys, ", "))
	}

	// Build query with dynamic token range filter
	queryBase := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", keyspace, table)
	whereClauses := []string{}
	var args []interface{}

	if tr.Start != "nil" {
		whereClauses = append(whereClauses, fmt.Sprintf("TOKEN(%s) >= ?", partitionKeyStr))
		args = append(args, tr.Start)
	}
	if tr.End != "nil" {
		whereClauses = append(whereClauses, fmt.Sprintf("TOKEN(%s) < ?", partitionKeyStr))
		args = append(args, tr.End)
	}

	query := queryBase
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	var count int64
	err := session.Query(query, args...).WithContext(ctx).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("range query failed: %w", err)
	}
	return count, nil
}
