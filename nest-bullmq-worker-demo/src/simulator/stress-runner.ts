import { spawn } from 'node:child_process';
import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { config } from '../shared/config.js';

type StressConfig = {
  workers: number;
  conversations: number;
  messagesPerConversation: number;
};

function parseArgs(): StressConfig {
  return {
    workers: Number(process.argv[2] ?? '3'),
    conversations: Number(process.argv[3] ?? '30'),
    messagesPerConversation: Number(process.argv[4] ?? '200')
  };
}

async function run(command: string, args: string[], label: string): Promise<void> {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: 'inherit', shell: true });
    child.on('exit', (code) => {
      if (code === 0) {
        resolve(undefined);
      } else {
        reject(new Error(`${label} failed with code=${code}`));
      }
    });
    child.on('error', reject);
  });
}

async function waitForQueueDrain(queueName: string, timeoutMs: number): Promise<boolean> {
  const redisConnection = new Redis(config.redisUrl);
  const queue = new Queue(queueName, { connection: redisConnection });
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const counts = await queue.getJobCounts('waiting', 'active', 'delayed');
    if (counts.waiting === 0 && counts.active === 0 && counts.delayed === 0) {
      await queue.close();
      await redisConnection.quit();
      return true;
    }
    await new Promise((r) => setTimeout(r, 2000));
  }

  await queue.close();
  await redisConnection.quit();
  return false;
}

async function main(): Promise<void> {
  const cfg = parseArgs();
  console.log(`[stress] starting ${cfg.workers} worker processes`);

  const workerProcesses: ReturnType<typeof spawn>[] = [];
  for (let i = 1; i <= cfg.workers; i += 1) {
    const workerId = `worker-${i}`;
    const child = spawn('npx', ['tsx', 'src/worker/runner.ts', workerId], {
      stdio: 'inherit',
      shell: true,
      env: { ...process.env, WORKER_ID: workerId },
      cwd: process.cwd()
    });
    workerProcesses.push(child);
  }

  await new Promise((r) => setTimeout(r, 4000));

  await run(
    'npx',
    ['tsx', 'src/simulator/load-generator.ts', String(cfg.conversations), String(cfg.messagesPerConversation)],
    'load-generator'
  );

  console.log('[stress] load sent. Waiting for queue to drain...');
  const drained = await waitForQueueDrain(config.queueName, 120000);
  console.log(`[stress] queue drain result: ${drained ? 'drained' : 'timeout'}`);

  console.log('[stress] letting workers flush metrics (5s)...');
  await new Promise((r) => setTimeout(r, 5000));

  for (const worker of workerProcesses) {
    worker.kill('SIGINT');
  }

  await new Promise((r) => setTimeout(r, 3000));

  await run('npx', ['tsx', 'src/simulator/report-generator.ts'], 'report-generator');

  console.log('[stress] completed');
}

void main();
