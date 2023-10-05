/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/store"
	sdk "github.com/cosmos/cosmos-sdk/types"
	distTypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	"github.com/strangelove-ventures/lens/client"
	"github.com/strangelove-ventures/lens/client/chain_registry"
	"github.com/strangelove-ventures/lens/cmd"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
)

type Transaction struct {
	BlockNumber int64  `parquet:"name=block_number, type=INT64"`
	Time        int64  `parquet:"name=time, type=INT64"`
	MessageType string `parquet:"name=message_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Source      string `parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Destination string `parquet:"name=destination, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Amount      int64  `parquet:"name=amount, type=INT64"`
}

func HandleMsg(logger *zap.Logger, msg sdk.Msg, msgIndex int, height int64, hash []byte, client *client.ChainClient) {
	//distTypes.MsgWithdrawDelegatorReward
	switch m := msg.(type) {
	case *distTypes.MsgWithdrawDelegatorReward:
		print("withdraw ", m.DelegatorAddress, " amount ", m.Size)
		// err := i.InsertMsgTransferRow(hash, m.Token.Denom, m.SourceChannel, m.Route(), m.Token.Amount.String(), m.Sender,
		// 	client.MustEncodeAccAddr(m.GetSigners()[0]), m.Receiver, m.SourcePort, msgIndex)
		// if err != nil {
		// 	logger.Info("Failed to insert MsgTransfer", "index", msgIndex, "height", height, "err", err.Error())
		// }
	default:
		// TODO: do we need to do anything here?
	}

}

func IndexMsg(logger *zap.Logger, pw *writer.ParquetWriter, msg sdk.Msg, msgIndex int, height int64, hash []byte, client *client.ChainClient) {
	switch m := msg.(type) {
	case *distTypes.MsgWithdrawDelegatorReward:
		print("withdraw ", m.DelegatorAddress, " amount ", m.Size)
		// err := i.InsertMsgTransferRow(hash, m.Token.Denom, m.SourceChannel, m.Route(), m.Token.Amount.String(), m.Sender,
		// 	client.MustEncodeAccAddr(m.GetSigners()[0]), m.Receiver, m.SourcePort, msgIndex)
		// if err != nil {
		// 	logger.Info("Failed to insert MsgTransfer", "index", msgIndex, "height", height, "err", err.Error())
		// }
	default:
		logger.Error("unknown message type", zap.String("type", fmt.Sprintf("%T", m)))
	}
}

func readDatabase(chainClient *client.ChainClient, logger *zap.Logger) {
	//db, err := leveldb.OpenFile("/Users/richard/workspace/flappy_trade/data/application.db", nil)
	db, err := leveldb.OpenFile("/Users/richard/workspace/somm-bucket/.sommelier/data/blockstore.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	metaKey := []byte(fmt.Sprintf("H:%v", 1936108))
	meta, err := db.Get(metaKey, nil)
	if err != nil {
		log.Fatal(err)
	}
	logger.Info("meta", zap.String("meta", string(meta)))

	prefix := "P:"
	//iter := db.NewIterator(nil, nil)
	iter := db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()
	for iter.Next() {
		/*key := iter.Key()

		}*/
		//BH = block hash
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		if bytes.HasPrefix(key, []byte("BH")) {
			//block height to hash mapping
			//ignore
		} else if bytes.HasPrefix(key, []byte("C:")) {
			//fmt.Printf("Key: %s\n", key)
			//commit information
		} else if bytes.HasPrefix(key, []byte("H:")) {
			fmt.Printf("Key: %s\n", key)
		} else if bytes.HasPrefix(key, []byte("P:")) {
			fmt.Printf("Key: %s\n", string(key))
			fmt.Printf("Value: %s\n", string(iter.Value()))
			//block part.
			//tx := iter.Value()
			//var block interface{}
			//var protoCodec = encoding.GetCodec(proto.Name)
			//block := &distTypes.Part{}
			//err = protoCodec.Unmarshal(tx, block)
			//sdkTx, err := chainClient.Codec.TxConfig.TxDecoder()(tx)
			if err != nil {
				// TODO application specific txs fail here (e.g. DEX swaps, Akash deployments, etc.)
				fmt.Printf("[key %s] Failed to decode tx. Err: %s \n", string(key), err.Error()) //block.Block.Height, index+1, len(block.Block.Data.Txs), err.Error())
				continue
			}
			fmt.Printf("block key %s tx : ..", string(key))
			/*for msgIndex, msg := range sdkTx.GetMsgs() {
				print(msg)
				HandleMsg(logger, msg, msgIndex, 1, []byte{}, chainClient)
			}*/
		} else if bytes.HasPrefix(key, []byte("SC:")) {
			//stored commit
			fmt.Printf("Key: %s\n", key)
		} else if bytes.HasPrefix(key, []byte("blockStore")) {
			//meta data about the block chain such as current height
			//fmt.Printf("Key: %s, Value: %s\n", key, iter.Value())
		} else {
			//fmt.Printf("Key: %s\n", key)
			fmt.Printf("Key: %s, Value: %s\n", key, iter.Value())
		}
	}
}

func readStore(chainClient *client.ChainClient, logger *zap.Logger) {
	storedb, err := dbm.NewGoLevelDB("blockstore", "/Users/richard/workspace/somm-bucket/.sommelier/data")
	if err != nil {
		logger.Error("failed to open blockstore", zap.Error(err))
		log.Fatal(err)
	}
	defer storedb.Close()

	// Initialize Parquet file for writing
	parquetFilePath := "/Users/richard/workspace/training-data/sommelier.parquet"
	// Initialize Parquet Writer
	fw, err := local.NewLocalFileWriter(parquetFilePath)
	if err != nil {
		log.Fatalf("Failed to create Parquet writer: %v", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(Transaction), 2)
	if err != nil {
		log.Fatalf("Failed to create Parquet writer: %v", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	store := store.NewBlockStore(storedb)
	block := store.LoadBlock(1936108)
	if block == nil {
		fmt.Printf("block is nil")
	} else {
		logger.Info("found block at time", zap.Int64("block", block.Height), zap.String("time", block.Time.UTC().String()))
		for index, tx := range block.Txs {
			sdkTx, err := chainClient.Codec.TxConfig.TxDecoder()(tx)
			if err != nil {
				// TODO application specific txs fail here (e.g. DEX swaps, Akash deployments, etc.)
				fmt.Printf("[Height %d] {%d/%d txs} - Failed to decode tx. Err: %s \n", block.Height, index+1, len(block.Data.Txs), err.Error())
				// gravity.v1.MsgSubmitEthereumTxConfirmation:
				//https://github.com/PeggyJV/gravity-bridge/tree/main/module/proto/gravity/v1
				continue
			}
			fmt.Printf("block %d tx %d: %s\n", block.Height, index, tx)
			for msgIndex, msg := range sdkTx.GetMsgs() {
				print(msg)
				IndexMsg(logger, pw, msg, msgIndex, block.Height, tx.Hash(), chainClient)
			}
		}
	}

}

func findEarliestBlock(rpc string, logger *zap.Logger) {
	print(rpc)
	chainID := "sommelier-3"
	curBlock := int64(5)
	chainClient, err := client.NewChainClient(logger, &client.ChainClientConfig{ChainID: chainID, RPCAddr: rpc, KeyringBackend: "test"}, os.Getenv("HOME"), os.Stdin, os.Stdout)
	if err != nil {
		logger.Error("failed to build new chain client for %s. err: %v", zap.String("chain", chainID), zap.Error(err))
	}
	for i := int64(5); i < 12000000; i += 1000000 {
		block, err := chainClient.RPCClient.Block(context.Background(), &curBlock)
		if err != nil {
			logger.Error("failed to get block %d. err: %v", zap.Error(err), zap.String("rpc", rpc))
		} else {
			logger.Info("We have a winner on ", zap.String("rpc", rpc), zap.String("block", block.Block.Header.Time.UTC().String()))
			break
		}
	}
}

func checkConnection(rpc string, logger *zap.Logger) bool {
	print(rpc)
	//use 5 in case there is some weirdness early on.
	//testBlock := int64(10848174)
	testBlock := int64(2030037)
	chainId := "sommelier-3"
	chainClient, err := client.NewChainClient(logger, &client.ChainClientConfig{ChainID: "sommelier-3", RPCAddr: rpc, KeyringBackend: "test"}, os.Getenv("HOME"), os.Stdin, os.Stdout)
	if err != nil {
		logger.Error("failed to build new chain client for %s. err: %v", zap.String("chain", chainId), zap.Error(err))
	}
	block, err := chainClient.RPCClient.Block(context.Background(), &testBlock)
	if err != nil {
		logger.Error("failed to get block %d. err: %v", zap.Error(err), zap.String("rpc", rpc))
	} else {
		logger.Info("We have a winner on ", zap.String("rpc", rpc), zap.String("block", block.Block.Header.Time.UTC().String()))
		print(block.Block.Header.Time.UTC().String())
		return true
	}
	return false
}

func main() {
	var logger, _ = zap.NewDevelopment()
	/// read the blockchain myself.

	/// try syning through others
	chainInfo, err := chain_registry.DefaultChainRegistry(logger).GetChain(context.Background(), "sommelier")
	if err != nil {
		log.Fatalf("failed to get chain info. err: %v", err)
	}

	rpcList, err := chainInfo.GetAllRPCEndpoints()
	if err != nil {
		logger.Error("failed to get RPC endpoints on chain", zap.String("chain", chainInfo.ChainName), zap.Error(err))
	}

	//rpcList = []string{"tcp://34.30.89.183:26657"}
	//rpcList = []string{"https://sommelier-mainnet-rpc.autostake.com:443"}
	for _, rpc := range rpcList {
		print(rpc)
		//checkConnection(rpc, logger)
		//findEarliestBlock(rpc, logger)
	}
	//this will need to change each time the endpoint is reset - check cloud console
	//rpc := "tcp://34.30.89.183:26657"
	rpc := "https://sommelier-mainnet-rpc.autostake.com:443"
	//	Use Chain info to select random endpoint
	/*rpc, err := chainInfo.GetRandomRPCEndpoint(context.Background())
	if err != nil {
		logger.Error("failed to get RPC endpoints on chain", zap.String("chain", chainInfo.ChainName), zap.Error(err))
	}
	*/
	chainClient, err := client.NewChainClient(logger, &client.ChainClientConfig{ChainID: "sommelier-3", RPCAddr: rpc, KeyringBackend: "test"}, os.Getenv("HOME"), os.Stdin, os.Stdout)
	if err != nil {
		logger.Error("failed to build new chain client for %s. err: %v", zap.String("chain", chainInfo.ChainID), zap.Error(err))
	}
	//readDatabase(chainClient, logger)
	readStore(chainClient, logger)

	//lets find if anyone has block 1.
	//var blocks = []int64{10760228, 10760229, 10760230, 10760231, 10760232, 10760233, 10760234, 10760235, 10760236}
	//a := int64(10760228)
	// run the indexer
	//start_index := 1936000
	start_index := 10938228
	for a := int64(start_index); a < int64(start_index+2000); a++ {
		block, err := chainClient.RPCClient.Block(context.Background(), &a)
		if err != nil {
			logger.Error("failed to get block %d. err: %v", zap.Error(err))
		} else {
			//wrong
			logger.Info("found block at time", zap.Int64("block", a), zap.String("time", block.Block.Header.Time.UTC().String()))
			for index, tx := range block.Block.Txs {
				sdkTx, err := chainClient.Codec.TxConfig.TxDecoder()(tx)
				if err != nil {
					// TODO application specific txs fail here (e.g. DEX swaps, Akash deployments, etc.)
					fmt.Printf("[Height %d] {%d/%d txs} - Failed to decode tx. Err: %s \n", block.Block.Height, index+1, len(block.Block.Data.Txs), err.Error())
					continue
				}
				fmt.Printf("block %d tx %d: %s\n", a, index, tx)
				for msgIndex, msg := range sdkTx.GetMsgs() {
					print(msg)
					HandleMsg(logger, msg, msgIndex, block.Block.Height, tx.Hash(), chainClient)
				}
			}
		}
	}
	cmd.Execute()

}
