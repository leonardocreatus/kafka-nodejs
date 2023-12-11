import { Kafka } from 'kafkajs'
import express from 'express'
import { faker } from '@faker-js/faker'
import fs from 'node:fs'

const file = fs.readFileSync('../config.json', 'utf8')
const { ip } = JSON.parse(file)

const app = express();

const kafka = new Kafka({
    clientId: 'kafka',
    brokers: [`${ip}:9092`]
})

const producer = kafka.producer()
await producer.connect()

app.get('/', async (req, res) => {
    await producer.send({
        topic: 'input',
        messages: [
          { value: JSON.stringify({firstName: faker.person.firstName(), lastName: faker.person.lastName()}) },
        ],
    })
    res.send()
})

app.listen(3000, () => {
    console.log('Producer running at 3000')
})