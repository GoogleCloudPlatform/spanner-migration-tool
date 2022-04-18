package summary

import (
	"encoding/json"
	"net/http"
)

// getSummary returns table wise summary of conversion.
func GetSummary(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(getSummary())
}
