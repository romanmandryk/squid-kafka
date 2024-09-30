import { Store } from '@subsquid/typeorm-store'
import { Entity, EntityClass, FindManyOptions, FindOneOptions } from '@subsquid/typeorm-store/lib/store'
import { Producer } from 'kafka-node'
import { FindOptionsWhere, EntityTarget } from 'typeorm'


export class KafkaStore {
  private kafkaProducer: Producer
  private kafkaTopic: string

  constructor(producer: Producer, topic: string) {
    this.kafkaProducer = producer
    this.kafkaTopic = topic
  }
  
  private stringifyWithBigInt(obj: any): string {
    return JSON.stringify(obj, (_, value) =>
      typeof value === 'bigint' ? value.toString() : value
    );
  }

  async save<E extends Entity>(entityOrEntities: E | E[]): Promise<void> {
    const items = Array.isArray(entityOrEntities) ? entityOrEntities : [entityOrEntities];
    // Send the data to Kafka
    const messages = items.map(item => ({
      key: item.id,
      value: this.stringifyWithBigInt(item)
    }))
    
    return new Promise((resolve, reject) => {
        this.kafkaProducer.send([{
            topic: this.kafkaTopic,
            messages
          }], (err, data) => {
          if (err) reject(err)
          else resolve(data)
        })
      })
  }

  async insert<E extends Entity>(entityOrEntities: E | E[]): Promise<void> {
    const items = Array.isArray(entityOrEntities) ? entityOrEntities : [entityOrEntities];
    await this.save(items);
  } 


  async saveMany(entities: Entity[]): Promise<void> {
    throw new Error('Method not implemented.')
  }

  getFkSignature(): string {
    throw new Error('Method not implemented.')
  }

  async upsertMany(entities: Entity[]): Promise<void> {
    throw new Error('Method not implemented.')
  }

  upsert<E extends Entity>(entity: E): Promise<void>
  upsert<E extends Entity>(entities: E[]): Promise<void>
  upsert(entities: unknown): Promise<void> {
    throw new Error('Method not implemented.')
  }
  remove<E extends Entity>(entity: E): Promise<void>
  remove<E extends Entity>(entities: E[]): Promise<void>
  remove<E extends Entity>(entityClass: EntityClass<E>, id: string | string[]): Promise<void>
  remove(entityClass: unknown, id?: unknown): Promise<void> {
    throw new Error('Method not implemented.')
  }
  count<E extends Entity>(entityClass: EntityClass<E>, options?: FindManyOptions<E>): Promise<number> {
    throw new Error('Method not implemented.')
  }
  countBy<E extends Entity>(entityClass: EntityClass<E>, where: FindOptionsWhere<E> | FindOptionsWhere<E>[]): Promise<number> {
    throw new Error('Method not implemented.')
  }
  find<E extends Entity>(entityClass: EntityClass<E>, options?: FindManyOptions<E>): Promise<E[]> {
    throw new Error('Method not implemented.')
  }
  findBy<E extends Entity>(entityClass: EntityClass<E>, where: FindOptionsWhere<E> | FindOptionsWhere<E>[]): Promise<E[]> {
    throw new Error('Method not implemented.')
  }
  findOne<E extends Entity>(entityClass: EntityClass<E>, options: FindOneOptions<E>): Promise<E | undefined> {
    throw new Error('Method not implemented.')
  }
  findOneBy<E extends Entity>(entityClass: EntityClass<E>, where: FindOptionsWhere<E> | FindOptionsWhere<E>[]): Promise<E | undefined> {
    throw new Error('Method not implemented.')
  }
  findOneOrFail<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E> {
    throw new Error('Method not implemented.')
  }
  findOneByOrFail<E extends Entity>(entityClass: EntityTarget<E>, where: FindOptionsWhere<E> | FindOptionsWhere<E>[]): Promise<E> {
    throw new Error('Method not implemented.')
  }
  get<E extends Entity>(entityClass: EntityClass<E>, optionsOrId: FindOneOptions<E> | string): Promise<E | undefined> {
    throw new Error('Method not implemented.')
  }
  
}
