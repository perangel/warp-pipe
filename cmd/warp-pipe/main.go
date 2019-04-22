package main

import (
	"log"

	"github.com/perangel/warp-pipe/internal/cli"
)

func main() {
	if err := cli.WarpPipeCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
