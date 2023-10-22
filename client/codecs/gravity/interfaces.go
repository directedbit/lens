package gravity

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/gogo/protobuf/proto"
)

// EthereumTxConfirmation represents one validtors signature for a given
// outgoing ethereum transaction
type EthereumTxConfirmation interface {
	proto.Message

	GetSigner() common.Address
	GetSignature() []byte
	GetStoreIndex() []byte
	Validate() error
}
