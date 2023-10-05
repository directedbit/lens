package client

import (
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/std"
	"github.com/cosmos/cosmos-sdk/types/module"
	authTx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	distTypes "github.com/cosmos/cosmos-sdk/x/distribution/types"

	ethermintcodecs "github.com/strangelove-ventures/lens/client/codecs/ethermint"
	//gravitycodecs "github.com/strangelove-ventures/lens/client/codecs/gravity"

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
	//for sommelier
	distTypes.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	//gravity
	//gravitycodecs.RegisterInterfaces(encodingConfig.InterfaceRegistry)
	//gravitycodecs.HelloWorld()

	for _, c := range extraCodecs {
		switch c {
		case "ethermint":
			ethermintcodecs.RegisterInterfaces(encodingConfig.InterfaceRegistry)
			encodingConfig.Amino.RegisterConcrete(&ethermintcodecs.PubKey{}, ethermintcodecs.PubKeyName, nil)
			encodingConfig.Amino.RegisterConcrete(&ethermintcodecs.PrivKey{}, ethermintcodecs.PrivKeyName, nil)
		case "injective":
			injectivecodecs.RegisterInterfaces(encodingConfig.InterfaceRegistry)
			encodingConfig.Amino.RegisterConcrete(&injectivecodecs.PubKey{}, injectivecodecs.PubKeyName, nil)
			encodingConfig.Amino.RegisterConcrete(&injectivecodecs.PrivKey{}, injectivecodecs.PrivKeyName, nil)
		}
	}

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
