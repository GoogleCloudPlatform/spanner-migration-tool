package csv

import (
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func ToSpannerType(columnType string) (ddl.Type, error) {
	ty := strings.ToUpper(columnType)
	switch {
	case ty == "BOOL":
		return ddl.Type{Name: ddl.Bool}, nil
	// In case user enters the length as well, ex: BYTES(40).
	case strings.HasPrefix(ty, "BYTES"):
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case ty == "DATE":
		return ddl.Type{Name: ddl.Date}, nil
	case ty == "FLOAT64" || ty == "FLOAT":
		return ddl.Type{Name: ddl.Float64}, nil
	case ty == "INT64" || ty == "INT":
		return ddl.Type{Name: ddl.Int64}, nil
	case ty == "NUMERIC":
		return ddl.Type{Name: ddl.Numeric}, nil
	// In case user enters the length as well, ex: STRING(40).
	case strings.HasPrefix(ty, "STRING"):
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case ty == "TIMESTAMP":
		return ddl.Type{Name: ddl.Timestamp}, nil
	case ty == "JSON":
		return ddl.Type{Name: ddl.JSON}, nil
	default:
		return ddl.Type{}, fmt.Errorf("%v is not a valid Spanner column type", columnType)
	}
}
