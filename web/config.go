package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	ProjectID  string `json:"ProjectID"`
	InstanceID string `json:"InstanceID"`
}

func getConfigFromJson() Config {

	content, err := ioutil.ReadFile("./config.json")

	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}

	var c Config

	err = json.Unmarshal(content, &c)

	if err != nil {
		log.Fatal("Error during Unmarshal(): ", err)
	}

	return c
}

func setconfigInJson(c Config) {

	f, err := os.OpenFile("./web/config.json", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	file, _ := json.MarshalIndent(c, "", " ")

	f.Write(file)

}

//getConfigFromEnv gets configuration from environment variables
// and save it in config.json file.
func getConfigFromEnv() {

	var c Config
	c.InstanceID = os.Getenv("InstanceID")
	c.ProjectID = os.Getenv("ProjectID")

	fmt.Println("InstanceID from environment : ", c.InstanceID)
	fmt.Println("ProjectID from environment : ", c.ProjectID)

	f, err := os.OpenFile("./web/config.json", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	file, _ := json.MarshalIndent(c, "", " ")

	f.Write(file)

}

/*
using gcloud
$gcloud config get project
$gcloud config set project
$gcloud config set spanner/instance appdev-ps1
gcloud config get spanner/instance
*/
