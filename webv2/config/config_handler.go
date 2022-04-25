package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

//Config represents Spanner Configiuration for Spanner Session Management.
type Config struct {
	GCPProjectID      string `json:"GCPProjectID"`
	SpannerInstanceID string `json:"SpannerInstanceID"`
}

// getConfig returns configurations.
func GetConfig(w http.ResponseWriter, r *http.Request) {
	content, err := GetSpannerConfig()
	if err != nil {
		http.Error(w, "Data access error", http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(content)
}

// setSpannerConfig sets Spanner Config.
func SetSpannerConfig(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}

	var c Config
	err = json.Unmarshal(reqBody, &c)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	err = saveSpannerConfigFile(c)
	if err != nil {
		log.Println(err)
		http.Error(w, "Data access error", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(c)
}
