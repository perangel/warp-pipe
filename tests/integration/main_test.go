package integration

import (
	"flag"
	"fmt"
	"os"
	"testing"
)

var (
	runIntegrationTests = flag.Bool("integration", false, "run integration tests (default false)")
)

func TestMain(m *testing.M) {
	flag.Parse() // Parse() must be called explicitly, see TestMain() docs.

	if !*runIntegrationTests {
		fmt.Println("Integration tests disabled. Use -integration flag to enable.")
		return
	}

	exitVal := m.Run()

	os.Exit(exitVal)
}
