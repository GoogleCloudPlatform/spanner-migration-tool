package import_file

import (
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"os"
)

func ResetReader(dumpReader *os.File, fileUri string) (*os.File, error) {
	_, err := dumpReader.Seek(0, 0)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't reset reader: %v\n", err))
		dumpReader.Close()
		dumpReader, err = os.Open(fileUri)
	}
	return dumpReader, err
}
