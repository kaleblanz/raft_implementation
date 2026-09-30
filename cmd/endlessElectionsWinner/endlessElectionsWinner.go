package main

import (
	//"fmt"
	"os"
	"strconv"

	"cse586.raftm/api"
	"cse586.raftm/impl"
)

/*
input is:
cmd/endlessElectionsWinner/endlessElectionsWinner 0 data/second_timeout_server.json
cmd/endlessElectionsWinner/endlessElectionsWinner 1 data/second_timeout_server.json
cmd/endlessElectionsWinner/endlessElectionsWinner 2 data/second_timeout_server.json
*/

func main() {
	if len(os.Args) != 3 {
		//fmt.Fprintln(os.Stderr, "usage: server <num> <configfile>")
		os.Exit(1)
	}

	num, err := strconv.Atoi(os.Args[1])
	if err != nil {
		//fmt.Fprintf(os.Stderr, "Error parsing server number: %v\n", err)
	}

	cfg, err := api.ParseClusterConfig(os.Args[2])
	if err != nil {
		//	fmt.Fprintf(os.Stderr, "Error parsing configuration: %v\n", err)
		os.Exit(1)
	}

	if num >= len(cfg.Servers) {
		//fmt.Fprintf(os.Stderr, "Invalid server %d (max %d)\n", num, len(cfg.Servers)-1)
		os.Exit(1)
	}

	server, err := impl.NewRafTMServer(num, &cfg)
	if err != nil || server == nil {
		//fmt.Fprintf(os.Stderr, "Error creating server: %v\n", err)
		os.Exit(1)
	}

	// Block until EOF on standard input, then exit, shutting down the server
	os.Stdin.Read(make([]byte, 1))

	server.Shutdown()
}
