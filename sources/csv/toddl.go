package csv

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func ToSpannerType(columnType string) (ddl.Type, error) {
	ty := strings.ToUpper(columnType)
	switch {
	case ty == "BOOL":
		return ddl.Type{Name: ddl.Bool}, nil
	// We accept variations including BYTES, BYTES(), BYTES(0) since the length doesn't matter.
	case strings.HasPrefix(ty, "BYTES"):
		match, _ := regexp.MatchString(`^BYTES\([0-9]*\)$`, ty)
		if match || ty == "BYTES" {
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
		return ddl.Type{}, fmt.Errorf("%v is not a valid Spanner column type", columnType)
	case ty == "DATE":
		return ddl.Type{Name: ddl.Date}, nil
	case ty == "FLOAT64":
		return ddl.Type{Name: ddl.Float64}, nil
	case ty == "INT64":
		return ddl.Type{Name: ddl.Int64}, nil
	case ty == "NUMERIC":
		return ddl.Type{Name: ddl.Numeric}, nil
	// We accept variations including STRING, STRING(), STRING(0) since the length doesn't matter.
	case strings.HasPrefix(ty, "STRING"):
		match, _ := regexp.MatchString(`^STRING\([0-9]*\)$`, ty)
		if match || ty == "STRING" {
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
		return ddl.Type{}, fmt.Errorf("%v is not a valid Spanner column type", columnType)
	case ty == "TIMESTAMP":
		return ddl.Type{Name: ddl.Timestamp}, nil
	case ty == "JSON":
		return ddl.Type{Name: ddl.JSON}, nil
	default:
		return ddl.Type{}, fmt.Errorf("%v is not a valid Spanner column type", columnType)
	}
}
