package gravity

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
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
