package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type Config struct {
	GCPProjectID      string `json:"GCPProjectID"`
	SpannerInstanceID string `json:"SpannerInstanceID"`
}

// getConfig returns configurations.
func getConfig(w http.ResponseWriter, r *http.Request) {

	content, err := getConfigForSpanner()

	if err != nil {
		http.Error(w, "Data access error", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(content)
}

// setSpannerConfig sets Spanner Config.
func setSpannerConfig(w http.ResponseWriter, r *http.Request) {

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}

	var c Config
	err = json.Unmarshal(reqBody, &c)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	err = setSpannerConfigFile(c)
	if err != nil {
		http.Error(w, "Data access error", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(c)
}

//getConfigForSpanner reads configuration from configuration file.
func getConfigForSpanner() (Config, error) {

	var c Config

	content, err := ioutil.ReadFile("./web/config.json")
	if err != nil {
		fmt.Println(err)
		return c, err
	}

	err = json.Unmarshal(content, &c)
	if err != nil {
		return c, err
	}
	return c, nil
}

//setSpannerConfigFile saves spanner configuration in configuration file.
func setSpannerConfigFile(c Config) error {

	f, err := os.OpenFile("./web/config.json", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	file, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		return err
	}

	_, err = f.Write(file)
	if err != nil {
		return err
	}
	return nil
}

//getConfigFromEnv gets configuration from environment variables
// when harbourbridge is loading first time.
// and save it in /web/config.json file.
func getConfigFromEnv() {

	var c Config
	c.GCPProjectID = os.Getenv("GCPProjectID")
	c.SpannerInstanceID = os.Getenv("SpannerInstanceID ")

	if c.GCPProjectID == "" || c.SpannerInstanceID == "" {

		log.Println("warning : please set GCPProjectID and SpannerInstanceID as environment variables")
	}

	f, err := os.OpenFile("./web/config.json", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)

	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()

	file, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		fmt.Println(err)
	}
	_, err = f.Write(file)

	if err != nil {
		fmt.Println(err)
	}
}
