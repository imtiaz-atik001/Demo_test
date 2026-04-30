import { WorkerConfig } from './types.js';

function readNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export const config: WorkerConfig = {
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(','),
  kafkaClientId: process.env.KAFKA_CLIENT_ID ?? 'conversation-worker-client',
  kafkaGroupId: process.env.KAFKA_GROUP_ID ?? 'conversation-worker-group',
  topicMain: process.env.TOPIC_MAIN ?? 'conversation.events',
  topicRetryFast: process.env.TOPIC_RETRY_FAST ?? 'conversation.events.retry.1s',
  topicRetrySlow: process.env.TOPIC_RETRY_SLOW ?? 'conversation.events.retry.5s',
  topicDlq: process.env.TOPIC_DLQ ?? 'conversation.events.dlq',
  partitions: readNumber('PARTITIONS', 100),
  processingDelayMs: readNumber('PROCESSING_DELAY_MS', 120),
  maxRetries: readNumber('MAX_RETRIES', 3),
  workerId: process.env.WORKER_ID ?? `worker-${Math.random().toString(16).slice(2, 8)}`
};
