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
	"path/filepath"
	"strings"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/store"
	cometbft "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/authz"

	//authzTypes "github.com/cosmos/cosmos-sdk/x/authz/types"
	bankTypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distTypes "github.com/cosmos/cosmos-sdk/x/distribution/types"

	govTypes "github.com/cosmos/cosmos-sdk/x/gov/types/v1beta1"
	upgradeTypes "github.com/cosmos/cosmos-sdk/x/upgrade/types"

	//ibcTypes "github.com/cosmos/cosmos-sdk/x/ibc/core/04-channel/types"
	stakeTypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	transfertypes "github.com/cosmos/ibc-go/v7/modules/apps/transfer/types"
	"github.com/strangelove-ventures/lens/client"
	"github.com/strangelove-ventures/lens/client/chain_registry"
	"github.com/strangelove-ventures/lens/client/codecs/gravity"
	"github.com/strangelove-ventures/lens/cmd"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
)

// need to use as raw data as possible, can always do transforms on
// the data later.
type Transaction struct {
	BlockNumber     int64   `parquet:"name=block_number, type=INT64"`
	Time            int64   `parquet:"name=time, type=INT64"`
	MessageType     string  `parquet:"name=message_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Source          string  `parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Destination     string  `parquet:"name=destination, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Amount          int64   `parquet:"name=amount, type=INT64"`
	OsmoToEthPrice  float32 `parquet:"name=osmo_eth_price, type=FLOAT"`
	SommToOsmoPrice float32 `parquet:"name=somm_osmo_price, type=FLOAT"`
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

func IndexMsg(logger *zap.Logger, pw *writer.ParquetWriter, chainClient *client.ChainClient, msg sdk.Msg, msgIndex int, height int64, blockTime int64) bool {
	var newTrans = Transaction{}
	switch m := msg.(type) {
	case *bankTypes.MsgSend:
		print("MsgSend ", m.Amount[0].Amount.String(), " of ", m.Amount[0].Denom, " from ", m.FromAddress, " to ", m.ToAddress)
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "bank.MsgSend",
			Source:      m.FromAddress,
			Destination: m.ToAddress,
			Amount:      m.Amount[0].Amount.Int64(),
		}
	case *stakeTypes.MsgCreateValidator:
		print("ignoring MsgCreateValidator ", m.Value.Amount.String(), " from ", m.DelegatorAddress, " to ", m.ValidatorAddress)
	case *stakeTypes.MsgEditValidator:
		print("ignoring MsgEditValidator ")

	case *transfertypes.MsgTransfer:
		print("IbcMsgTransfer ", m.Token.Amount.String(), " from ", m.Sender, " to ", m.Receiver)
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "ibc.MsgTransfer",
			Source:      m.Sender,
			Destination: m.Receiver,
			Amount:      m.Token.Amount.Int64(),
		}
	case *distTypes.MsgFundCommunityPool:
		print("MsgFundCommunityPool ", m.Amount.AmountOf("usomm").Int64(), " from ", m.Depositor)
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "distribution.MsgFundCommunityPool",
			Source:      m.Depositor,
			Destination: "community",
			Amount:      m.Amount.AmountOf("usomm").Int64(),
		}

	case *distTypes.MsgWithdrawDelegatorReward:
		print("MsgWithdrawDelegatorReward ", m.DelegatorAddress, " amount ", m.Size)
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "distribution.MsgWithdrawDelegatorReward",
			Source:      m.ValidatorAddress,
			Destination: m.DelegatorAddress,
			Amount:      0,
		}
	case *distTypes.MsgWithdrawValidatorCommission:
		print("MsgWithdrawValidatorCommission ", m.ValidatorAddress)
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "distribution.MsgWithdrawValidatorCommission",
			Source:      m.ValidatorAddress,
			Destination: m.ValidatorAddress,
			Amount:      0}
	case *stakeTypes.MsgDelegate:
		print("MsgDelegate ", m.Amount.String(), " from ", m.DelegatorAddress, " to ", m.ValidatorAddress)
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "staking.MsgDelegate",
			Source:      m.DelegatorAddress,
			Destination: m.ValidatorAddress,
			Amount:      m.Amount.Amount.Int64(),
		}
	case *stakeTypes.MsgUndelegate:
		print("MsgUndelegate ", m.Amount.String(), " from ", m.DelegatorAddress, " to ", m.ValidatorAddress)
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "staking.MsgUndelegate",
			Source:      m.ValidatorAddress,
			Destination: m.DelegatorAddress,
			Amount:      m.Amount.Amount.Int64(),
		}
	case *authz.MsgRevoke:
		print("Ignoring MsgRevoke ", m.Grantee, " from ", m.Granter, " don't think it will impact price")
	case *authz.MsgGrant:
		print("ignoring MsgGrant ", m.Granter, " to ", m.Grantee, " don't think it will impact price")
	case *stakeTypes.MsgBeginRedelegate:
		print("ignoring MsgBeginRedelegate ", m.Amount.String(), " from ", m.DelegatorAddress, " to ", m.ValidatorDstAddress)
	case *authz.MsgExec:
		for i := range m.Msgs {
			var innerMsg sdk.Msg
			if err := chainClient.Codec.InterfaceRegistry.UnpackAny(m.Msgs[i], &innerMsg); err != nil {
				logger.Error("failed to unpack msg", zap.Error(err))
				panic(err)
			}
			IndexMsg(logger, pw, chainClient, innerMsg, i, height, blockTime)
		}

	case *govTypes.MsgSubmitProposal:
		newTrans = Transaction{
			BlockNumber: height,
			Time:        blockTime,
			MessageType: "gov.MsgSubmitProposal",
			Source:      m.Proposer,
			Destination: "community",
			Amount:      m.InitialDeposit[0].Amount.Int64(),
		}
	case *govTypes.MsgVote:
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "gov.MsgVote",
			Source:      m.Voter,
			Destination: "community",
			Amount:      0,
		}
	case *govTypes.MsgVoteWeighted:
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "gov.MsgVoteWeighted",
			Source:      m.Voter,
			Destination: "community",
			Amount:      m.Options[0].Weight.RoundInt64(),
		}
	case *upgradeTypes.MsgSoftwareUpgrade:
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "upgrade.MsgSoftwareUpgrade",
			Source:      m.Authority,
			Destination: "community",
			Amount:      0,
		}
	case *upgradeTypes.MsgCancelUpgrade:
		newTrans = Transaction{BlockNumber: height,
			Time:        blockTime,
			MessageType: "upgrade.MsgCancelUpgrade",
			Source:      m.Authority,
			Destination: "community",
			Amount:      0,
		}
	case *gravity.MsgSubmitEthereumTxConfirmation:
		print("ignoring Ethereum Tx type ", m.Confirmation.TypeUrl, " amount ", m.Confirmation.Value)
	default:
		logger.Fatal("unknown message type", zap.String("type", fmt.Sprintf("%T", m)))
		return false
	}
	if (newTrans != Transaction{}) {
		if err := pw.Write(newTrans); err != nil {
			log.Fatal("Failed writing to parquet file:", err)
		}
		return true
	}
	return false
}

func readCorruptDatabase() {
	dbPath := "/Users/richard/workspace/somm-bucket/notional/data/blockstore.db"
	// Repair the DB
	db, err := leveldb.RecoverFile(dbPath, nil)
	if err != nil {
		log.Fatalf("Recover failed: %v", err)
	}
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	counter := 0
	for iter.Next() {
		key := iter.Key()
		fmt.Printf("Key: %s\n", key)
		counter++
		if counter > 100 {
			break
		}
	}
	db.Close()

}

func readDatabase(chainClient *client.ChainClient, logger *zap.Logger) {
	//db, err := leveldb.OpenFile("/Users/richard/workspace/flappy_trade/data/application.db", nil)
	//db, err := leveldb.OpenFile("/Users/richard/workspace/somm-bucket/.sommelier/data/blockstore.db", nil)
	db, err := leveldb.OpenFile("/Users/richard/workspace/somm-bucket/notional/data/blockstore.db", nil)
	//db, err := leveldb.OpenFile("/Users/richard/workspace/cometbft/blockstore.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	metaKey := []byte(fmt.Sprintf("H:%v", 1936108))
	meta, err := db.Get(metaKey, nil)
	if err != nil {
		logger.Error("failed to get meta", zap.Error(err))
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

func verifyParquetDB(logger *zap.Logger, deleteAtFinish bool) {

	filename := "/Users/richard/workspace/training-data/test.parquet"
	pw := initParquetWriter(filename)
	data := []Transaction{
		{BlockNumber: 1,
			Time:            2,
			MessageType:     "3",
			Source:          "4",
			Destination:     "5",
			Amount:          6,
			OsmoToEthPrice:  -1.0,
			SommToOsmoPrice: -1.0},
	}

	for _, testTransaction := range data {
		if err := pw.Write(testTransaction); err != nil {
			log.Fatal("Failed writing to parquet file:", err)
		}
	}
	if err := pw.Flush(true); err != nil {
		log.Fatal("Failed flushing to parquet file:", err)
	}
	if err := pw.WriteStop(); err != nil {
		log.Fatal("Failed writing to stop parquet file:", err)
	}
	pw.PFile.Close()

	pr := initParquetReader(filename)
	rows := int(pr.GetNumRows())
	for i := 0; i < rows; i++ {
		logger.Info("reading row", zap.Int("row", i))
		testTransaction := Transaction{}
		transactionRow := make([]Transaction, 1)
		if err := pr.Read(&transactionRow); err != nil {
			log.Fatal("Failed reading:", err)
		}
		for _, testTransaction = range transactionRow {
			logger.Info("read row", zap.Int64("block", testTransaction.BlockNumber), zap.Int64("time", testTransaction.Time), zap.String("message", testTransaction.MessageType), zap.String("source", testTransaction.Source), zap.String("destination", testTransaction.Destination), zap.Int64("amount", testTransaction.Amount))
			testTransaction.OsmoToEthPrice = 12.34
			//testTransaction.OsmoToEthPrice = 12.34
			testTransaction.SommToOsmoPrice = 23.78
		}
	}
	pr.ReadStop()
	if err := pr.PFile.Close(); err != nil {
		log.Fatal("Failed closing file:", err)
	}
	//now write the price data

	if deleteAtFinish {
		if err := os.Remove(filename); err != nil {
			log.Fatal("Failed removing parquet file:", err)
		}
	}
}

func initParquetReader(fileName string) *reader.ParquetReader {
	fr, err := local.NewLocalFileReader(fileName)
	if err != nil {
		log.Fatal("Can't open file", err)
	}
	pr, err := reader.NewParquetReader(fr, new(Transaction), 2)
	if err != nil {
		log.Fatal("Can't create parquet reader", err)
	}
	return pr
}

func initParquetWriter(fileName string) *writer.ParquetWriter {
	fw, err := local.NewLocalFileWriter(fileName)
	if err != nil {
		log.Fatalf("Failed to create Parquet writer: %v", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(Transaction), 2)
	if err != nil {
		log.Fatalf("Failed to create Parquet writer: %v", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	return pw
}

func getFileWithPrefix(folderPath string, prefix string) string {
	files, err := os.ReadDir(folderPath)
	if err != nil {
		log.Fatalf("Failed to read directory: %s", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue // Skip directories
		}

		// Using file.Name() to get the name of the file and then checking if it starts with the prefix
		if strings.HasPrefix(file.Name(), prefix) {
			fmt.Printf("Found a match: %s\n", file.Name())
			return file.Name()
		}
	}
	return ""
}

func getParquetWriter(rootDirectory string, block *cometbft.Block, logger *zap.Logger, curPw *writer.ParquetWriter, curFile string, curPrefix string) (*writer.ParquetWriter, string, string) {
	//ensure it's a UTC time
	t := time.Unix(block.Time.Unix(), 0).UTC()
	// Format the time to construct the folder and file name

	filePrefix := t.Format("02")
	if curPrefix == filePrefix && curPw != nil && curFile != "" {
		return curPw, curFile, curPrefix
	}
	folderPath := filepath.Join(rootDirectory, t.Format("./2006/01/"))
	// Check if the folder exists
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		// Create missing directories
		if err := os.MkdirAll(folderPath, os.ModePerm); err != nil {
			log.Fatalf("Could not create directory: %v", err)
		}
	}
	fileName := getFileWithPrefix(folderPath, filePrefix)
	if fileName == "" {
		fileName = filePrefix + "-" + fmt.Sprint(block.Height) + ".parquet"
	}
	// Combine folder and file names to get the full path
	fullPath := filepath.Join(folderPath, fileName)
	if curPw != nil {
		curPw.Flush(true)
		curPw.WriteStop()
		curPw.PFile.Close()
	}

	pw := initParquetWriter(fullPath)
	return pw, fullPath, filePrefix

}

func indexDbStore(chainClient *client.ChainClient, logger *zap.Logger) {
	//storedb, err := dbm.NewGoLevelDB("blockstore", "/Users/richard/workspace/somm-bucket/.sommelier/data")
	//storedb, err := dbm.NewGoLevelDB("blockstore", "/Users/richard/workspace/cometbft")
	storedb, err := dbm.NewGoLevelDB("blockstore", "/Volumes/harry/sommelier")
	partquetFolder := "/Users/richard/workspace/flappy_trade/parquet-training-data"
	if err != nil {
		logger.Error("failed to open blockstore", zap.Error(err))
		log.Fatal(err)
	}
	defer storedb.Close()

	store := store.NewBlockStore(storedb)
	// 25/1/2022 1936108
	// 26/1/2022 1938785
	// 27/1/2022 1955417
	// 2022/02/05-2104778.parquet
	// 2022/02/17-2304669.parquet
	// 2022/02/20-2354455.parquet
	// 2022/02/21-2370941.parquet
	// 2022/03/05-2566098.parquet
	// 2022/03/07-2597957.parquet
	// 2022/03/19-2790436.parquet
	// 2022/04/01-2985179.parquet
	// 2022/04/20-3263356.parquet
	// 2022/04/22-3292600.parquet
	// 2022/05/06-3496897.parquet
	// 2022/05/13-3598939.parquet
	// 2022/06/17-4098228.parquet
	start_block := int64(4098228)
	//end_block := start_block + 200000 //should be about a week
	//end_block := start_block + 30000 //should be about a day
	//end_block := start_block + 1000 //should be about an hour
	end_block := start_block + 2000000
	var pw *writer.ParquetWriter
	var curPwFile string
	var curPrefix string
	for i := start_block; i < end_block; i++ {
		block := store.LoadBlock(i)
		if block == nil {
			logger.Fatal("block is nil")
		} else {
			blockTime := block.Time.Unix()
			pw, curPwFile, curPrefix = getParquetWriter(partquetFolder, block, logger, pw, curPwFile, curPrefix)
			logger.Info("found block", zap.Int64("bh", block.Height), zap.Time("bt", time.Unix(blockTime, 0).UTC()))
			valid_blocks := 0
			for index, tx := range block.Txs {
				sdkTx, err := chainClient.Codec.TxConfig.TxDecoder()(tx)
				if err != nil {
					// /gravity.v1.SignerSetTxConfirmation
					// /gravity.v1.MsgSubmitEthereumEvent
					//
					if strings.Contains(err.Error(), "/cosmos.slashing.v1beta1.MsgUnjail") {
						logger.Info("Ignoring MsgUnjail")
					} else if strings.Contains(err.Error(), "/gravity.v1.MsgSubmitEthereumEvent") {
						logger.Info("Ignoring MsgSubmitEthereumEvent")
					} else if strings.Contains(err.Error(), "/gravity.v1.MsgEthereumHeightVote") {
						logger.Info("Ignoring MsgSubmitEthereumEvent")
					} else if strings.Contains(err.Error(), "/gravity.v1.MsgSendToEthereum") {
						logger.Info("Ignoring MsgSendToEthereum")
					} else if strings.Contains(err.Error(), "/gravity.v1.MsgRequestBatchTx") {
						logger.Info("Ignoring MsgRequestBatchTx")
					} else if strings.Contains(err.Error(), "/gravity.v1.BatchTxConfirmation") {
						logger.Info("Ignoring BatchTxConfirmation")
					} else if strings.Contains(err.Error(), "/ibc.core.client.v1.MsgCreateClient") {
						logger.Info("Ignoring IBC MsgCreateClient")
					} else if strings.Contains(err.Error(), "/ibc.core.client.v1.MsgUpdateClient") {
						logger.Info("Ignoring IBC MsgCreateClient")
					} else if strings.Contains(err.Error(), "ibc.core.channel.v1.MsgAcknowledgement") {
						logger.Info("Ignoring IBC MsgAcknowledgement")
					} else if strings.Contains(err.Error(), "/ibc.core.channel.v1.MsgTimeout") {
						logger.Info("Ignoring IBC MsgTimeout")
					} else if strings.Contains(err.Error(), "/ibc.core.channel.v1.MsgRecvPacket:") {
						logger.Info("Ignoring IBC MsgRecvPacket, #TODO inspect the data packet to see if it has amounts etc")
					} else if strings.Contains(err.Error(), "/cosmos.authz.v1beta1.MsgGrant:") {
						logger.Info("Ignoring Authz MsgGrant, authorises  someone to do something on someone else's behalf seems unlikely to impact price")
					} else if strings.Contains(err.Error(), "/gravity.v1.MsgDelegateKeys:") {
						logger.Info("Ignoring gravity MsgDelegateKeys")
					} else if strings.Contains(err.Error(), "cosmos.feegrant.v1beta1.MsgGrantAllowance") {
						logger.Info("Ignoring feegrant MsgGrantAllowance")
					} else {
						// TODO application specific txs fail here (e.g. DEX swaps, Akash deployments, etc.)
						fmt.Printf("[Height %d] {%d/%d txs} - Failed to decode tx. Err: %s \n", block.Height, index+1, len(block.Data.Txs), err.Error())
						// gravity.v1.MsgSubmitEthereumTxConfirmation:
						//https://github.com/PeggyJV/gravity-bridge/tree/main/module/proto/gravity/v1
						logger.Fatal("[Height %d] {%d/%d txs} - Failed to decode tx. Err: %s \n", zap.Int64("height", block.Height), zap.Int("index", index+1), zap.Int("total", len(block.Data.Txs)), zap.Error(err))
					}
				} else {
					fmt.Printf("block %d tx %d: %s\n", block.Height, index, tx)
					for msgIndex, msg := range sdkTx.GetMsgs() {
						print(msg)
						if IndexMsg(logger, pw, chainClient, msg, msgIndex, block.Height, blockTime) {
							valid_blocks++
						}
					}
				}
			}
			if valid_blocks == 0 {
				emptyTransaction := Transaction{BlockNumber: block.Height,
					Time:        blockTime,
					MessageType: "empty_block",
					Source:      "",
					Destination: "",
					Amount:      0,
				}
				if err := pw.Write(emptyTransaction); err != nil {
					log.Fatal("Failed writing to parquet file:", err)
				}
			}
		}
		//pw.Flush(true)
	}
	//pw.WriteStop()
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

func indexFromRPC(logger *zap.Logger, chainClient *client.ChainClient) {
	start_index := 10938228
	for a := int64(start_index); a < int64(start_index+2000); a++ {
		logger.Info("Indexing block ", zap.Int64("block", a))
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
					logger.Fatal("[Height %d] {%d/%d txs} - Failed to decode tx. Err: %s \n", zap.Int64("height", block.Block.Height), zap.Int("index", index+1), zap.Int("total", len(block.Block.Data.Txs)), zap.Error(err))
					//continue
				}
				fmt.Printf("block %d tx %d: %s\n", a, index, tx)
				for msgIndex, msg := range sdkTx.GetMsgs() {
					print(msg)
					HandleMsg(logger, msg, msgIndex, block.Block.Height, tx.Hash(), chainClient)
				}
			}
		}
	}
}

func main() {
	var logger, _ = zap.NewDevelopment()
	/// read the blockchain myself.
	verifyParquetDB(logger, true)

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
	indexDbStore(chainClient, logger)
	//readCorruptDatabase()
	//readDatabase(chainClient, logger)
	logger.Info("done")
	return

	//lets find if anyone has block 1.
	//var blocks = []int64{10760228, 10760229, 10760230, 10760231, 10760232, 10760233, 10760234, 10760235, 10760236}
	//a := int64(10760228)
	// run the indexer
	//start_index := 1936000
	indexFromRPC(logger, chainClient)

	cmd.Execute()

}
