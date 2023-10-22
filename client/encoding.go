package client

import (
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/std"
	"github.com/cosmos/cosmos-sdk/types/module"
	authTx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authz "github.com/cosmos/cosmos-sdk/x/authz"
	bankTypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distTypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	govTypes "github.com/cosmos/cosmos-sdk/x/gov/types/v1beta1"
	stakeTypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	upgradeTypes "github.com/cosmos/cosmos-sdk/x/upgrade/types"
	transfertypes "github.com/cosmos/ibc-go/v7/modules/apps/transfer/types"

	ethermintcodecs "github.com/strangelove-ventures/lens/client/codecs/ethermint"
	gravitycodecs "github.com/strangelove-ventures/lens/client/codecs/gravity"

	//sommeliercodecs "github.com/peggyjv/sommelier/v6/x/cork/keeper"

	//gravitycodecs "gravitycodecs/codecs/mygravitycodecs"
	//gravitycodecs "github.com/directedbit/gravitycodecs"
	//gravitycodecs "github.com/peggyjv/gravity-bridge/module/v4/x/gravity/types"

	injectivecodecs "github.com/strangelove-ventures/lens/client/codecs/injective"
)

type Codec struct {
	InterfaceRegistry types.InterfaceRegistry
	Marshaler         codec.Codec
	TxConfig          client.TxConfig
	Amino             *codec.LegacyAmino
}

func MakeCodec(moduleBasics []module.AppModuleBasic, extraCodecs []string) Codec {
	modBasic := module.NewBasicManager(moduleBasics...)
	encodingConfig := MakeCodecConfig()
	std.RegisterLegacyAminoCodec(encodingConfig.Amino)
	std.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	modBasic.RegisterLegacyAminoCodec(encodingConfig.Amino)
	modBasic.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	//cosmos types..
	distTypes.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	bankTypes.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	stakeTypes.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	transfertypes.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	authz.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	govTypes.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	upgradeTypes.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	//gravity
	gravitycodecs.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	//gravitycodecs.HelloWorld()

	ethermintcodecs.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	encodingConfig.Amino.RegisterConcrete(&ethermintcodecs.PubKey{}, ethermintcodecs.PubKeyName, nil)
	encodingConfig.Amino.RegisterConcrete(&ethermintcodecs.PrivKey{}, ethermintcodecs.PrivKeyName, nil)
	injectivecodecs.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	encodingConfig.Amino.RegisterConcrete(&injectivecodecs.PubKey{}, injectivecodecs.PubKeyName, nil)
	encodingConfig.Amino.RegisterConcrete(&injectivecodecs.PrivKey{}, injectivecodecs.PrivKeyName, nil)

	return encodingConfig
}

func MakeCodecConfig() Codec {
	interfaceRegistry := types.NewInterfaceRegistry()
	marshaler := codec.NewProtoCodec(interfaceRegistry)
	return Codec{
		InterfaceRegistry: interfaceRegistry,
		Marshaler:         marshaler,
		TxConfig:          authTx.NewTxConfig(marshaler, authTx.DefaultSignModes),
		Amino:             codec.NewLegacyAmino(),
	}
}
