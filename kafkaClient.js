const { KafkaClient, Consumer } = require('kafka-node');
const dotenv = require('dotenv');

dotenv.config();
const KAFKA_TOPIC = 'decoded-data2'
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


    const consumer = new Consumer(
      client,
      [{ topic: topic, partition: 0 }],
      {
        autoCommit: false,
        fromOffset: true
      }
    );

    consumer.on('message', (message) => {       
      console.log('Received message:');
      console.log(message);
   
    });

    consumer.on('error', (err) => {
      reject(err);
    });



