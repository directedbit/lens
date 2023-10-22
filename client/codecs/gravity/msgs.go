package gravity

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum/common"
)

const (
	_ = iota
	SignerSetTxPrefixByte
	BatchTxPrefixByte
	ContractCallTxPrefixByte
)

func (msg MsgSubmitEthereumTxConfirmation) ValidateBasic() (err error) {
	return nil
}

// GetSigners defines whose signature is required
func (msg MsgSubmitEthereumTxConfirmation) GetSigners() []sdk.AccAddress {
	// TODO: figure out how to convert between AccAddress and ValAddress properly
	acc, err := sdk.AccAddressFromBech32(msg.Signer)
	if err != nil {
		panic(err)
	}
	return []sdk.AccAddress{acc}
}

func (u *SignerSetTxConfirmation) GetSigner() common.Address {
	return common.HexToAddress(u.EthereumSigner)
}

func MakeSignerSetTxKey(nonce uint64) []byte {
	return append([]byte{SignerSetTxPrefixByte}, sdk.Uint64ToBigEndian(nonce)...)
}

func (sstx *SignerSetTxConfirmation) GetStoreIndex() []byte {
	return MakeSignerSetTxKey(sstx.SignerSetNonce)
}

func (u *SignerSetTxConfirmation) Validate() error {
	return nil
}
