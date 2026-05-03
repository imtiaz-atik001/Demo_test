import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { config } from '../shared/config.js';

async function main(): Promise<void> {
  const redisConnection = new Redis(config.redisUrl);
  const queue = new Queue(config.queueName, { connection: redisConnection });
  const dlqQueue = new Queue(config.dlqName, { connection: redisConnection });

  const before = await queue.getJobCounts('waiting', 'active', 'delayed', 'completed', 'failed');
  console.log(`[clean] before: waiting=${before.waiting} active=${before.active} delayed=${before.delayed} completed=${before.completed} failed=${before.failed}`);

  await queue.obliterate({ force: true }).catch((e) => console.error('[clean] obliterate main queue failed:', e.message));
  await dlqQueue.obliterate({ force: true }).catch((e) => console.error('[clean] obliterate DLQ failed:', e.message));

  // Also clean sequence trackers and locks
  const keys = await redisConnection.keys('seq:conversation:*');
  const lockKeys = await redisConnection.keys('lock:conversation:*');
  if (keys.length > 0) await redisConnection.del(...keys);
  if (lockKeys.length > 0) await redisConnection.del(...lockKeys);

  console.log(`[clean] removed ${keys.length} sequence trackers and ${lockKeys.length} locks`);
  console.log('[clean] queue cleaned');

  await queue.close();
  await dlqQueue.close();
  await redisConnection.quit();
}

void main();
