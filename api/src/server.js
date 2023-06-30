import express from 'express';
import { Kafka, Partitioners } from "kafkajs";

import routes from "./routes";

const app = express();

const kafka = new Kafka({
    clientId: 'api',
    brokers: ['localhost:9092'],
    retry: {
        initialRetryTime: 400,
        retries: 10
    }
})

const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })
const consumer = kafka.consumer({ groupId: 'certificate-group-receiver' })

app.use((req, res, next) => {
    req.producer = producer

    return next();
});

app.use(routes);

async function run() {
    await producer.connect()
    await consumer.connect()

    await consumer.subscribe({ topic: 'certification-response' })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log('Resposta', String(message.value))
        }
    })
    app.listen(3000)
}

run().catch(console.error)
