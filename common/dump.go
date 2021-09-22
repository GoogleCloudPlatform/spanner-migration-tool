package common

import (
	"fmt"
	"os"
)

// OpenDump returns a file descriptor to the dump file if parameter has been
// passed by the user. If no parameter has been passed, then return stdin.
func OpenDump(dumpFile string) *os.File {
	if dumpFile != "" {
		fmt.Printf("\nloading dump file from path: %s\n", dumpFile)
		file, err := os.Open(dumpFile)
		if err != nil {
			fmt.Printf("\nerror reading file: %v err:%v", dumpFile, err)
			panic(err)
		}
		return file
	}
	return os.Stdin
}
