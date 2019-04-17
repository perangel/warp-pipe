package main

import (
	"log"

	"github.com/perangel/warp-pipe/pkg/cli"
)

func main() {
	if err := cli.WarpPipeCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
