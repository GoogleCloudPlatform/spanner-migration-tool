package utils

import (
	"fmt"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

// updateSessionFile updates the content of session file with
// latest sessionState.Conv while also dumping schemas and report.
func UpdateSessionFile() error {
	sessionState := session.GetSessionState()

	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	_, err := conversion.WriteConvGeneratedFiles(sessionState.Conv, sessionState.DbName, sessionState.Driver, ioHelper.BytesRead, ioHelper.Out)
	if err != nil {
		return fmt.Errorf("encountered error %w. Cannot write files", err)
	}
	return nil
}
