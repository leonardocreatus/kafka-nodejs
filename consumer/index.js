import { Kafka } from 'kafkajs'
import fs from 'node:fs'

const file = fs.readFileSync('../config.json', 'utf8')
const { ip } = JSON.parse(file)


const kafka = new Kafka({
    clientId: 'kafka',
    brokers: [`${ip}:9092`]
})

const consumer = kafka.consumer({ groupId: 'consumer' })

await consumer.connect()

await consumer.subscribe({ topic: 'output-topic', fromBeginning: false })

await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log({
            value: message.value.toString(),
        })
    },
})