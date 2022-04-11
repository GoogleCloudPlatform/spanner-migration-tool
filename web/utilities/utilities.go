package utilities

import "fmt"

func GetSessionFilePath(dbName string) string {
	dirPath := "harbour_bridge_output"
	return fmt.Sprintf("%s/%s.session.json", dirPath, dbName)
}
