/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package main is the entrypoint for the orderer binary
// and calls only into the server.Main() function.  No other
// function should be included in this package.
package main

import (
	"fmt"

	"github.com/hyperledger/fabric/config"
	"github.com/hyperledger/fabric/orderer/common/server"
)

func main() {

	// Read in config
	if err := config.ReadConfig("log.json"); err != nil {
		fmt.Printf("%s\n", err)
	}

	server.Main()
}
