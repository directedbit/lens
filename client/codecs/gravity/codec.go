package gravity

import (
	//codectypes "oldsdk/codec/types"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	//"github.com/strangelove-ventures/lens/client/codecs/gravity"
	//ovtypes "github.com/cosmos/cosmos-sdk/x/gov/types"
	//"github.com/strangelove-ventures/lens/client/codecs/gravity"
	//gravityTypes "github.com/peggyjv/gravity-bridge/module/v2/x/gravity/types"
)

func RegisterInterfaces(registry codectypes.InterfaceRegistry) {
	//gravity.v1.MsgSubmitEthereumTxConfirmation
	//didn't work :
	//registry.RegisterInterface("gravity.v1.MsgSubmitEthereumTxConfirmation", (*sdk.Msg)(nil))
	//from the project
	registry.RegisterImplementations((*sdk.Msg)(nil),
		&MsgSubmitEthereumTxConfirmation{},
		//&MsgSubmitEthereumEvent{},
	)

	registry.RegisterInterface(
		"gravity.v1.EthereumSignature",
		(*EthereumTxConfirmation)(nil),
		//&BatchTxConfirmation{},
		//&ContractCallTxConfirmation{},
		&SignerSetTxConfirmation{},
	)
}
