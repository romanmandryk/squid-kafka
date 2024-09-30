import {EvmBatchProcessor} from '@subsquid/evm-processor'
import * as usdcAbi from './abi/usdc'
import {UsdcTransfer} from './model'
import { KafkaWithTypORMDatabase } from './KafkaWithTypeORMDatabase'
import * as dotenv from 'dotenv'

dotenv.config()

const USDC_CONTRACT_ADDRESS = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
const KAFKA_TOPIC = 'decoded-data2'
const processor = new EvmBatchProcessor()
  // SQD Network gateways are the primary source of blockchain data in
  // squids, providing pre-filtered data in chunks of roughly 1-10k blocks.
  // Set this for a fast sync.
  .setGateway('https://v2.archive.subsquid.io/network/ethereum-mainnet')
  // Another data source squid processors can use is chain RPC.
  // In this particular squid it is used to retrieve the very latest chain data
  // (including unfinalized blocks) in real time. It can also be used to
  //   - make direct RPC queries to get extra data during indexing
  //   - sync a squid without a gateway (slow)
  .setRpcEndpoint('https://rpc.ankr.com/eth')
  // The processor needs to know how many newest blocks it should mark as "hot".
  // If it detects a blockchain fork, it will roll back any changes to the
  // database made due to orphaned blocks, then re-run the processing for the
  // main chain blocks.
  .setFinalityConfirmation(75)
  // .addXXX() methods request data items. In this case we're asking for
  // Transfer(address,address,uint256) event logs emitted by the USDC contract.
  //
  // We could have omitted the "address" filter to get Transfer events from
  // all contracts, or the "topic0" filter to get all events from the USDC
  // contract, or both to get all event logs chainwide. We also could have
  // requested some related data, such as the parent transaction or its traces.
  //
  // Other .addXXX() methods (.addTransaction(), .addTrace(), .addStateDiff()
  // on EVM) are similarly feature-rich.
  .addLog({
    range: { from: 20_000_000 }, //1 June 2024
    address: [USDC_CONTRACT_ADDRESS],
    topic0: [usdcAbi.events.Transfer.topic],
  })
  // .setFields() is for choosing data fields for the selected data items.
  // Here we're requesting hashes of parent transaction for all event logs.
  .setFields({
    log: {
      transactionHash: true,
    },
  })

const db = new KafkaWithTypORMDatabase({
  kafkaClientOptions: { kafkaHost: process.env.KAFKA_HOST || 'localhost:9092' },
  topic: KAFKA_TOPIC,
  supportHotBlocks: true
})
processor.run(db, async (ctx) => {
  console.log("processor.run")
  for (let block of ctx.blocks) {
    // On EVM, each block has four iterables - logs, transactions, traces,
    // stateDiffs
    for (let log of block.logs) {
      if (log.address === USDC_CONTRACT_ADDRESS &&
          log.topics[0] === usdcAbi.events.Transfer.topic) {
        // SQD's very own EVM codec at work - about 20 times faster than ethers
        let {from, to, value} = usdcAbi.events.Transfer.decode(log)
        const transfer = new UsdcTransfer({
          id: log.id,
          block: block.header.height,
          from,
          to,
          value,
          txnHash: log.transactionHash
        })
        //insert into kafka (individually to avoid message size limit)
        await ctx.store.insert(transfer)
        //transfers.push(transfer)

      }
    }
  }

  // Just one insert per batch!
  //console.log("transfers", transfers.length)
  //await ctx.store.insert(transfers)
})
