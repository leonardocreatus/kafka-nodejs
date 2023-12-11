
import { KafkaStreams } from "kafka-streams";
import fs from 'node:fs'

const file = fs.readFileSync('../config.json', 'utf8')
const { ip } = JSON.parse(file)


const config = {
    "noptions": {
        "metadata.broker.list": `${ip}:9092`,
        "group.id": "kafka-streams-test-native",
        "client.id": "kafka-streams-test-name-native",
        "event_cb": true,
        "compression.codec": "snappy",
        "api.version.request": true,
        "socket.keepalive.enable": true,
        "socket.blocking.max.ms": 100,
        "enable.auto.commit": false,
        "auto.commit.interval.ms": 100,
        "heartbeat.interval.ms": 250,
        "retry.backoff.ms": 250,
        "fetch.min.bytes": 100,
        "fetch.message.max.bytes": 2 * 1024 * 1024,
        "queued.min.messages": 100,
        "fetch.error.backoff.ms": 100,
        "queued.max.messages.kbytes": 50,
        "fetch.wait.max.ms": 1000,
        "queue.buffering.max.ms": 1000,
        "batch.num.messages": 10000
    },
    "tconf": {
        "auto.offset.reset": "earliest",
        "request.required.acks": 1
    },
    "batchOptions": {
        "batchSize": 5,
        "commitEveryNBatch": 1,
        "concurrency": 1,
        "commitSync": false,
        "noBatchCommits": false
    }
}

const factory = new KafkaStreams(config);

const kafkaTopicName = "input";
const stream = factory.getKStream(kafkaTopicName);

stream
    .mapJSONConvenience()
    .tap(console.log)
    .map(({ value }) => {
        return `${value.firstName} ${value.lastName}`
    })
    .tap(console.log)
    .wrapAsKafkaValue()
    .to("output-topic")

stream.start().then(() => {
    console.log("stream started, as kafka consumer is ready.");
}, error => {
    console.log("streamed failed to start: " + error);
});


