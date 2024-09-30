import { KafkaClient, Consumer } from 'kafka-node';
import dotenv from 'dotenv';

dotenv.config();
const KAFKA_TOPIC = 'decoded-data'
const kafkaConfig = {
  kafkaHost: process.env.KAFKA_BROKERS || 'localhost:9092',
  ssl: process.env.KAFKA_USE_SSL === 'true',
  sasl: process.env.KAFKA_USE_SASL === 'true' ? {
    mechanism: 'plain',
    username: process.env.KAFKA_SASL_USERNAME || '',
    password: process.env.KAFKA_SASL_PASSWORD || '',
  } : undefined,
};

const topic = KAFKA_TOPIC

const client = new KafkaClient(kafkaConfig);

function getMessageCount(): Promise<number> {
  return new Promise((resolve, reject) => {
    const consumer = new Consumer(client, [{ topic }], {
      autoCommit: false,
      fromOffset: true
    });

    consumer.on('error', (err) => {
      reject(err);
    });

    client.refreshMetadata([topic], (err) => {
      if (err) {
        reject(err);
        return;
      }

      const offsetRequest = {
        [topic]: [{
          partition: 0
        }]
      };

      client.sendOffsetFetchRequest(offsetRequest, (error, offsets) => {
        if (error) {
          reject(error);
          return;
        }

        const latestOffset = offsets[topic][0].offset;
        
        consumer.setOffset(topic, 0, -2);
        consumer.fetch((error, messages) => {
          if (error) {
            reject(error);
            return;
          }

          const earliestOffset = messages[0].offset;
          const messageCount = latestOffset - earliestOffset;

          consumer.close(true, () => {
            resolve(messageCount);
          });
        });
      });
    });
  });
}

async function main() {
  try {
    const count = await getMessageCount();
    console.log(`Number of messages in the Kafka topic '${topic}': ${count}`);
  } catch (error) {
    console.error('Error:', error);
  } finally {
    client.close(() => {
      console.log('Kafka client closed');
    });
  }
}

main();
