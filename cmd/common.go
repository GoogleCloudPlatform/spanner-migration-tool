package cmd

import (
	"encoding/csv"
	"fmt"
	"strings"
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