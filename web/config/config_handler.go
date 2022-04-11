package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/web/session"
	"github.com/cloudspannerecosystem/harbourbridge/web/shared"
)

//Config represents Spanner Configiuration for Spanner Session Management.
type Config struct {
	GCPProjectID      string `json:"GCPProjectID"`
	SpannerInstanceID string `json:"SpannerInstanceID"`
}

// getConfig returns configurations.
func GetConfig(w http.ResponseWriter, r *http.Request) {
	content, err := GetConfigForSpanner()
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

	err = setSpannerConfigFile(c)
	if err != nil {
		log.Println(err)
		http.Error(w, "Data access error", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(c)
}

//getConfigForSpanner reads configuration from configuration file.
func GetConfigForSpanner() (Config, error) {
	var c Config
	content, err := ioutil.ReadFile("./web/config.json")
	if err != nil {
		log.Println(err)
		return c, err
	}

	err = json.Unmarshal(content, &c)
	if err != nil {
		log.Println(err)
		return c, err
	}
	return c, nil
}

//setSpannerConfigFile saves spanner configuration in configuration file.
func setSpannerConfigFile(c Config) error {
	sessionState := session.GetSessionState()

	f, err := os.OpenFile("./web/config.json", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Println(err)
		return err
	}
	defer f.Close()

	file, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = f.Write(file)
	if err != nil {
		log.Println(err)
		return err
	}

	isValid := shared.PingMetadataDb(shared.GetSpannerUri(c.GCPProjectID, c.SpannerInstanceID))
	if !isValid {
		sessionState.IsOffline = true
		sessionState.GCPProjectID = ""
		sessionState.SpannerInstanceID = ""
	} else {
		sessionState.GCPProjectID = c.GCPProjectID
		sessionState.SpannerInstanceID = c.SpannerInstanceID
		sessionState.IsOffline = false
	}

	return nil
}

// getConfigFromEnv gets configuration from environment variables
// when harbourbridge is loading first time.
// and save it in /web/config.json file.
func GetConfigFromEnv() {
	var c Config
	c.GCPProjectID = os.Getenv("GCPProjectID")
	c.SpannerInstanceID = os.Getenv("SpannerInstanceID ")

	if c.GCPProjectID == "" || c.SpannerInstanceID == "" {
		log.Println("warning : please set GCPProjectID and SpannerInstanceID as environment variables")
	}

	f, err := os.OpenFile("./web/config.json", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	file, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		log.Println(err)
	}
	_, err = f.Write(file)

	if err != nil {
		log.Println(err)
	}
}
