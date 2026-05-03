import { WorkerConfig } from './types.js';

function readNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (!value) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export const config: WorkerConfig = {
  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
  queueName: process.env.QUEUE_NAME ?? 'conversation-events',
  dlqName: process.env.DLQ_NAME ?? 'conversation-events-dlq',
  processingDelayMs: readNumber('PROCESSING_DELAY_MS', 120),
  maxRetries: readNumber('MAX_RETRIES', 5),
  workerConcurrency: readNumber('WORKER_CONCURRENCY', 5),
  lockTtlSeconds: readNumber('LOCK_TTL_SECONDS', 30),
  workerId: process.env.WORKER_ID ?? `worker-${Math.random().toString(16).slice(2, 8)}`
};
