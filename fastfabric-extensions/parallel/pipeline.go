package parallel

import (
	"os"
	"strconv"

	"github.com/hyperledger/fabric/fastfabric-extensions/cached"
)

var (
	ReadyToCommit      chan chan *cached.Block
	ReadyForValidation chan *Pipeline
)

type Pipeline struct {
	Channel chan *cached.Block
	Block   *cached.Block
}

func InitPipeline() {

	strPipeline := os.Getenv("STREAMCHAIN_PIPELINE")

	p := 0

	if strPipeline != "" && strPipeline != "0" {
		p, _ = strconv.Atoi(strPipeline)
	}

	ReadyToCommit = make(chan chan *cached.Block, p)
	ReadyForValidation = make(chan *Pipeline, p)
}
