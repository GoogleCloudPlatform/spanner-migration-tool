package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/web/session"
	"github.com/cloudspannerecosystem/harbourbridge/web/shared"
)

func GetSpannerConfig() (Config, error) {
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

//  Gets configuration from environment variables and saves it in config file.
func LoadConfigFromEnv() {
	var c Config
	c.GCPProjectID = os.Getenv("GCPProjectID")
	c.SpannerInstanceID = os.Getenv("SpannerInstanceID")

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

// Saves spanner configuration in configuration file.
func saveSpannerConfigFile(c Config) error {

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

	sessionState := session.GetSessionState()
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
