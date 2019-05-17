package state

import (
	"sync"

	"github.com/hyperledger/fabric/fastfabric-extensions/cached"
	"github.com/hyperledger/fabric/fastfabric-extensions/parallel"
	common2 "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/state"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

var logger = util.GetLogger(util.ServiceLogger, "")

type ledgerResources interface {
	VerifyBlock(block *cached.Block, pvtdata util.PvtDataCollections) (*cached.Block, error)

	// StoreBlock deliver new block with underlined private data
	// returns missing transaction ids
	StoreBlock(block *cached.Block, data util.PvtDataCollections) error

	// StorePvtData used to persist private date into transient store
	StorePvtData(txid string, privData *transientstore.TxPvtReadWriteSetWithConfigInfo, blckHeight uint64) error

	// GetPvtDataAndBlockByNum get block by number and returns also all related private data
	// the order of private data in slice of PvtDataCollections doesn't imply the order of
	// transactions in the block related to these private data, to get the correct placement
	// need to read TxPvtData.SeqInBlock field
	GetPvtDataAndBlockByNum(seqNum uint64, peerAuthInfo common.SignedData) (*common.Block, util.PvtDataCollections, error)

	// Get recent block sequence number
	LedgerHeight() (uint64, error)

	// Close ledgerResources
	Close()
}

type GossipStateProviderImpl struct {
	state.GossipStateProvider
	chainID string
	buffer  parallel.PayloadsBuffer

	mediator *state.ServicesMediator
	ledgerResources
	done   sync.WaitGroup
	once   sync.Once
	stopCh chan struct{}
}

func NewGossipStateProvider(chainID string, services *state.ServicesMediator, ledger ledgerResources) state.GossipStateProvider {
	height, err := ledger.LedgerHeight()
	if height == 0 {
		// Panic here since this is an indication of invalid situation which should not happen in normal
		// code path.
		logger.Panic("Committer height cannot be zero, ledger should include at least one block (genesis).")
	}

	if err != nil {
		logger.Error("Could not read ledger info to obtain current ledger height due to: ", errors.WithStack(err))
		// Exiting as without ledger it will be impossible
		// to deliver new blocks
		return nil
	}

	gsp := &GossipStateProviderImpl{
		GossipStateProvider: state.NewGossipStateProvider(chainID, services, ledger),
		chainID:             chainID,
		mediator:            services,
		ledgerResources:     ledger,
		buffer:              parallel.NewPayloadsBuffer(height),
		stopCh:              make(chan struct{}, 1),
		once:                sync.Once{}}
	gsp.done.Add(1)

	parallel.InitPipeline()

	go gsp.deliverPayloads()
	return gsp
}

func (s *GossipStateProviderImpl) deliverPayloads() {

	for pipelinedBlock := range parallel.ReadyForValidation {
		go s.validate(pipelinedBlock)
	}
	logger.Debug("State provider has been stopped, finishing to push new blocks.")
	return
}

func (s *GossipStateProviderImpl) commit() {
	go s.store()

	for blockPromise := range parallel.ReadyToCommit {
		block, _ := <-blockPromise
		if block != nil {
			s.buffer.Push(block)
		}
	}
}
func (s *GossipStateProviderImpl) store() {
	defer s.done.Done()
	for {
		select {
		case <-s.buffer.Ready():
			block := s.buffer.Pop()
			// Commit block with available private transactions
			if err := s.ledgerResources.StoreBlock(block, util.PvtDataCollections{}); err != nil {
				logger.Errorf("Got error while committing(%+v)", errors.WithStack(err))
				return
			}

			// Update ledger height
			s.mediator.UpdateLedgerHeight(block.Header.Number+1, common2.ChainID(s.chainID))
			logger.Debugf("[%s] Committed block [%d] with %d transaction(s)",
				s.chainID, block.Header.Number, len(block.Data.Data))

		case <-s.stopCh:
			s.stopCh <- struct{}{}
			logger.Debug("State provider has been stopped, finishing to push new blocks.")
			return
		}
	}
}

func (s *GossipStateProviderImpl) validate(pipeline *parallel.Pipeline) {
	defer close(pipeline.Channel)
	if _, err := s.ledgerResources.VerifyBlock(pipeline.Block, util.PvtDataCollections{}); err != nil {
		logger.Errorf("Validation failed: %+v", err)
		return
	}
	pipeline.Channel <- pipeline.Block
}

func (s *GossipStateProviderImpl) Stop() {
	// Make sure stop won't be executed twice
	// and stop channel won't be used again

	s.once.Do(func() {
		s.stopCh <- struct{}{}
		// Make sure all go-routines has finished
		s.done.Wait()

	})
	s.GossipStateProvider.Stop()
}
