import { IsolationLevel, Store, TypeormDatabase } from '@subsquid/typeorm-store'
import { createOrmConfig} from '@subsquid/typeorm-config'
import { KafkaClient, Producer, KafkaClientOptions } from 'kafka-node'
import { UsdcTransfer } from './model/generated/usdcTransfer.model'
import { DatabaseState, FinalTxInfo, HashAndHeight } from '@subsquid/typeorm-store/lib/interfaces'
import { DataSource, EntityManager } from 'typeorm'
import { assertNotNull } from '@subsquid/util-internal'
import assert from 'assert'
import { KafkaStore } from './KafkaStore'

export class KafkaWithTypORMDatabase {
  private producer: Producer
  private topic: string
  private ksStatusSchema: string
  private ksIsolationLevel: IsolationLevel
  private ksCon?: DataSource
  private ksProjectDir: string
  private RACE_MSG = 'race condition detected: status update failed'

  constructor(options: {
    kafkaClientOptions: KafkaClientOptions
    topic: string
  } & ConstructorParameters<typeof TypeormDatabase>[0]) {    
    this.ksStatusSchema = options?.stateSchema || 'squid_processor'
    this.ksIsolationLevel = options?.isolationLevel || 'SERIALIZABLE'
    this.ksProjectDir = options?.projectDir || process.cwd()
    this.topic = options.topic
    const client = new KafkaClient(options.kafkaClientOptions)
    this.producer = new Producer(client)
  }


  private async sqlConnect(): Promise<DatabaseState> {    
    assert(this.ksCon == null, 'already connected')

    let cfg = createOrmConfig({projectDir: this.ksProjectDir})
    this.ksCon = new DataSource(cfg)

    await this.ksCon.initialize()

    try {
        return await this.ksCon.transaction('SERIALIZABLE', em => this.ksInitTransaction(em))
    } catch(e: any) {
        await this.ksCon.destroy().catch(() => {}) // ignore error
        this.ksCon = undefined
        throw e
    }    
  }
  async connect(): Promise<DatabaseState> {
    return await this.sqlConnect()    
  }

  async transact(info: FinalTxInfo, cb: (store: KafkaStore) => Promise<void>): Promise<void> {
    console.log("transact")
    return this.ksSubmit(async em => {
        let state = await this.ksGetState(em)
        let {prevHead: prev, nextHead: next} = info

        assert(state.hash === info.prevHead.hash, this.RACE_MSG)
        assert(state.height === prev.height)
        assert(prev.height < next.height)
        assert(prev.hash != next.hash)
        // todo rollback
        /* for (let i = state.top.length - 1; i >= 0; i--) {
            let block = state.top[i]
            await rollbackBlock(this.ksStatusSchema, em, block.height)
        } */
        let store = new KafkaStore(this.producer, this.topic)
        await cb(store)
        await this.ksUpdateStatus(em, state.nonce, next)
        console.log("ksUpdateStatus done", state.nonce, next)
    })  
  }
  

  private async ksSubmit(tx: (em: EntityManager) => Promise<void>): Promise<void> {
    let retries = 3
    while (true) {
        try {
            let con = this.ksCon
            assert(con != null, 'not connected')
            return await con.transaction(this.ksIsolationLevel, tx)
        } catch(e: any) {
            if (e.code == '40001' && retries) {
                retries -= 1
            } else {
                throw e
            }
        }
    }
  }

  private async ksGetState(em: EntityManager): Promise<DatabaseState> {
    let schema = this.ksEscapedSchema()

    let status: (HashAndHeight & {nonce: number})[] = await em.query(
        `SELECT height, hash, nonce FROM ${schema}.status WHERE id = 0`
    )

    assert(status.length == 1)

    let top: HashAndHeight[] = await em.query(
        `SELECT hash, height FROM ${schema}.hot_block ORDER BY height`
    )

    return assertStateInvariants({...status[0], top})
  }

  private async ksUpdateStatus(em: EntityManager, nonce: number, next: HashAndHeight): Promise<void> {
    let schema = this.ksEscapedSchema()

    let result: [data: any[], rowsChanged: number] = await em.query(
        `UPDATE ${schema}.status SET height = $1, hash = $2, nonce = nonce + 1 WHERE id = 0 AND nonce = $3`,
        [next.height, next.hash, nonce]
    )

    let rowsChanged = result[1]

    // Will never happen if isolation level is SERIALIZABLE or REPEATABLE_READ,
    // but occasionally people use multiprocessor setups and READ_COMMITTED.
    assert.strictEqual(
        rowsChanged,
        1,
        this.RACE_MSG
    )
  }

  private ksEscapedSchema(): string {
    let con = assertNotNull(this.ksCon)
    return con.driver.escape(this.ksStatusSchema)
  } 

  private async ksInitTransaction(em: EntityManager): Promise<DatabaseState> {
    let schema = this.ksEscapedSchema()

    await em.query(
        `CREATE SCHEMA IF NOT EXISTS ${schema}`
    )
    await em.query(
        `CREATE TABLE IF NOT EXISTS ${schema}.status (` +
        `id int4 primary key, ` +
        `height int4 not null, ` +
        `hash text DEFAULT '0x', ` +
        `nonce int4 DEFAULT 0`+
        `)`
    )
    await em.query( // for databases created by prev version of typeorm store
        `ALTER TABLE ${schema}.status ADD COLUMN IF NOT EXISTS hash text DEFAULT '0x'`
    )
    await em.query( // for databases created by prev version of typeorm store
        `ALTER TABLE ${schema}.status ADD COLUMN IF NOT EXISTS nonce int DEFAULT 0`
    )
    await em.query(
        `CREATE TABLE IF NOT EXISTS ${schema}.hot_block (height int4 primary key, hash text not null)`
    )
    await em.query(
        `CREATE TABLE IF NOT EXISTS ${schema}.hot_change_log (` +
        `block_height int4 not null references ${schema}.hot_block on delete cascade, ` +
        `index int4 not null, ` +
        `change jsonb not null, ` +
        `PRIMARY KEY (block_height, index)` +
        `)`
    )

    let status: (HashAndHeight & {nonce: number})[] = await em.query(
        `SELECT height, hash, nonce FROM ${schema}.status WHERE id = 0`
    )
    if (status.length == 0) {
        await em.query(`INSERT INTO ${schema}.status (id, height, hash) VALUES (0, -1, '0x')`)
        status.push({height: -1, hash: '0x', nonce: 0})
    }

    let top: HashAndHeight[] = await em.query(
        `SELECT height, hash FROM ${schema}.hot_block ORDER BY height`
    )

    return assertStateInvariants({...status[0], top})
  }
}

function assertStateInvariants(state: DatabaseState): DatabaseState {
    let height = state.height

    // Sanity check. Who knows what driver will return?
    assert(Number.isSafeInteger(height))

    assertChainContinuity(state, state.top)

    return state
  }

  function assertChainContinuity(base: HashAndHeight, chain: HashAndHeight[]) {
    let prev = base
    for (let b of chain) {
        assert(b.height === prev.height + 1, 'blocks must form a continues chain')
        prev = b
    }
  }