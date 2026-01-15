const express = require("express");
const { Kafka, logLevel } = require("kafkajs");

const PORT = Number(process.env.PORT || 8082);
const KAFKA_BROKERS_RAW = process.env.KAFKA_BROKERS || "kafka:9092";
const KAFKA_BROKERS = KAFKA_BROKERS_RAW.split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const TOPIC_MOVIE = "movie-events";
const TOPIC_USER = "user-events";
const TOPIC_PAYMENT = "payment-events";

const CONSUMER_GROUP_ID = process.env.KAFKA_CONSUMER_GROUP_ID || "events-service";

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function createKafkaClient() {
  return new Kafka({
    clientId: "events-service",
    brokers: KAFKA_BROKERS,
    logLevel: logLevel.NOTHING,
  });
}

async function startConsumer(kafka) {
  const consumer = kafka.consumer({ groupId: CONSUMER_GROUP_ID });
  await consumer.connect();

  await consumer.subscribe({ topic: TOPIC_MOVIE, fromBeginning: true });
  await consumer.subscribe({ topic: TOPIC_USER, fromBeginning: true });
  await consumer.subscribe({ topic: TOPIC_PAYMENT, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const payloadStr = message.value ? message.value.toString("utf8") : "";
      const payloadJson = safeJsonParse(payloadStr);

      console.log(
        JSON.stringify(
          {
            msg: "event-consumed",
            topic,
            partition,
            offset: message.offset,
            key: message.key ? message.key.toString("utf8") : null,
            payload: payloadJson ?? payloadStr,
          },
          null,
          2
        )
      );
    },
  });

  return consumer;
}

async function startProducer(kafka) {
  const producer = kafka.producer();
  await producer.connect();
  return producer;
}

function assertJsonBody(req, res) {
  if (req.body && typeof req.body === "object") return true;
  res.status(400).json({ error: "Invalid JSON body" });
  return false;
}

async function produceEvent(producer, topic, payload) {
  const value = JSON.stringify(payload);
  const results = await producer.send({
    topic,
    messages: [{ value }],
  });

  const first = results[0] || {};
  console.log(
    JSON.stringify(
      {
        msg: "event-produced",
        topic,
        partition: first.partition ?? null,
        baseOffset: first.baseOffset ?? null,
      },
      null,
      2
    )
  );
}

async function main() {
  const kafka = createKafkaClient();
  const producer = await startProducer(kafka);
  const consumer = await startConsumer(kafka);

  const app = express();
  app.use(express.json({ limit: "1mb" }));

  app.get("/api/events/health", (req, res) => {
    res.status(200).json({ status: true });
  });

  app.post("/api/events/movie", async (req, res) => {
    if (!assertJsonBody(req, res)) return;
    await produceEvent(producer, TOPIC_MOVIE, { type: "movie", ...req.body });
    res.status(201).json({ status: "success" });
  });

  app.post("/api/events/user", async (req, res) => {
    if (!assertJsonBody(req, res)) return;
    await produceEvent(producer, TOPIC_USER, { type: "user", ...req.body });
    res.status(201).json({ status: "success" });
  });

  app.post("/api/events/payment", async (req, res) => {
    if (!assertJsonBody(req, res)) return;
    await produceEvent(producer, TOPIC_PAYMENT, { type: "payment", ...req.body });
    res.status(201).json({ status: "success" });
  });

  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log(
      JSON.stringify(
        {
          msg: "events-service started",
          PORT,
          KAFKA_BROKERS,
          CONSUMER_GROUP_ID,
          topics: [TOPIC_MOVIE, TOPIC_USER, TOPIC_PAYMENT],
        },
        null,
        2
      )
    );
  });

  async function shutdown(signal) {
    console.log(JSON.stringify({ msg: "shutdown", signal }, null, 2));
    server.close();
    await Promise.allSettled([consumer.disconnect(), producer.disconnect()]);
    process.exit(0);
  }

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

