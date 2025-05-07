package import_file

import (
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"os"
)

func ResetReader(f *os.File, fileUri string) (*os.File, error) {
	_, err := f.Seek(0, 0)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't reset reader: %v\n", err))
		f.Close()
		return os.Open(fileUri)
	}
	return f, err

}
