package cmd

import (
<<<<<<< HEAD
<<<<<<< HEAD
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
=======
=======
	"context"
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
	"encoding/csv"
	"fmt"
	"os"
	"strings"
<<<<<<< HEAD
>>>>>>> 6522c9b (Add support for source-profile and target-profile in subcommands. (#208))
=======
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
)

// Parses input string `s` as a map of key-value pairs. It's expected that the
// input string is of the form "key1=value1,key2=value2,..." etc. Return error
// otherwise.
func parseProfile(s string) (map[string]string, error) {
	params := make(map[string]string)
	if len(s) == 0 {
		return params, nil
	}

	// We use CSV reader to parse key=value pairs separated by a comma to
	// handle the case where a value may contain a comma within a quote. We
	// expect exactly one record to be returned.
	r := csv.NewReader(strings.NewReader(s))
	r.Comma = ','
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return params, err
	}
	if len(records) > 1 {
		return params, fmt.Errorf("contains invalid newline characters")
	}

	for _, kv := range records[0] {
		s := strings.Split(strings.TrimSpace(kv), "=")
		if len(s) != 2 {
			return params, fmt.Errorf("invalid key=value pair (expected format: key1=value1): %v", kv)
		}
		if _, ok := params[s[0]]; ok {
			return params, fmt.Errorf("duplicate key found: %v", s[0])
		}
		params[s[0]] = s[1]
	}
	return params, nil
}
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))

func getResourceIds(ctx context.Context, targetProfile TargetProfile, now time.Time, driverName string, out *os.File) (string, string, string, error) {
	var err error
	project := targetProfile.conn.sp.project
	if project == "" {
		project, err = conversion.GetProject()
		if err != nil {
			return "", "", "", fmt.Errorf("can't get project: %v", err)
		}
	}
	fmt.Println("Using Google Cloud project:", project)

	instance := targetProfile.conn.sp.instance
	if instance == "" {
		instance, err = conversion.GetInstance(ctx, project, out)
		if err != nil {
			return "", "", "", fmt.Errorf("can't get instance: %v", err)
		}
	}
	fmt.Println("Using Cloud Spanner instance:", instance)
	conversion.PrintPermissionsWarning(driverName, out)

	dbName := targetProfile.conn.sp.dbname
	if dbName == "" {
		dbName, err = conversion.GetDatabaseName(driverName, now)
		if err != nil {
			return "", "", "", fmt.Errorf("can't get database name: %v", err)
		}
	}
	return project, instance, dbName, err
}
<<<<<<< HEAD
=======
>>>>>>> 6522c9b (Add support for source-profile and target-profile in subcommands. (#208))
=======
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
