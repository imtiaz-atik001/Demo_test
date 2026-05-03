import { readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { config } from '../shared/config.js';

type MetricsEntry = {
  type: 'metrics' | 'start' | 'end';
  workerId: string;
  ts: string;
  successCount?: number;
  failCount?: number;
  retryCount?: number;
  dlqCount?: number;
  lockContentionCount?: number;
  outOfOrderCount?: number;
  avgLatencyMs?: number;
  p50LatencyMs?: number;
  p95LatencyMs?: number;
  p99LatencyMs?: number;
  throughputPerSecond?: number;
  elapsedSeconds?: number;
};

type WorkerSummary = {
  workerId: string;
  successCount: number;
  failCount: number;
  retryCount: number;
  dlqCount: number;
  lockContentionCount: number;
  outOfOrderCount: number;
  avgLatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
};

const LOG_DIR = join(process.cwd(), 'consumer-logs');
const OUTPUT_FILE = join(LOG_DIR, 'stress-report.md');

async function loadWorkerMetrics(): Promise<WorkerSummary[]> {
  const files = await readdir(LOG_DIR).catch(() => [] as string[]);
  const workerFiles = files
    .filter((file) => file.endsWith('.jsonl'))
    .filter((file) => file !== 'stress-report.md')
    .sort((a, b) => a.localeCompare(b));

  const summaries: WorkerSummary[] = [];

  for (const file of workerFiles) {
    const content = await readFile(join(LOG_DIR, file), 'utf8');
    const lines = content.split(/\r?\n/).filter((line) => line.trim().length > 0);

    let latestMetrics: MetricsEntry | null = null;
    for (const line of lines) {
      try {
        const entry = JSON.parse(line) as MetricsEntry;
        if (entry.type === 'metrics') {
          latestMetrics = entry;
        }
      } catch {
        // ignore malformed lines
      }
    }

    if (latestMetrics) {
      summaries.push({
        workerId: latestMetrics.workerId,
        successCount: latestMetrics.successCount ?? 0,
        failCount: latestMetrics.failCount ?? 0,
        retryCount: latestMetrics.retryCount ?? 0,
        dlqCount: latestMetrics.dlqCount ?? 0,
        lockContentionCount: latestMetrics.lockContentionCount ?? 0,
        outOfOrderCount: latestMetrics.outOfOrderCount ?? 0,
        avgLatencyMs: latestMetrics.avgLatencyMs ?? 0,
        p95LatencyMs: latestMetrics.p95LatencyMs ?? 0,
        p99LatencyMs: latestMetrics.p99LatencyMs ?? 0
      });
    }
  }

  return summaries;
}

async function loadQueueStats(): Promise<{
  completed: number;
  failed: number;
  delayed: number;
  waiting: number;
  active: number;
  dlq: number;
}> {
  const redisConnection = new Redis(config.redisUrl);
  const queue = new Queue(config.queueName, { connection: redisConnection });
  const dlqQueue = new Queue(config.dlqName, { connection: redisConnection });

  const counts = await queue.getJobCounts('waiting', 'active', 'delayed', 'completed', 'failed');
  const dlqCounts = await dlqQueue.getJobCounts('waiting', 'completed');

  await queue.close();
  await dlqQueue.close();
  await redisConnection.quit();

  return {
    completed: counts.completed,
    failed: counts.failed,
    delayed: counts.delayed,
    waiting: counts.waiting,
    active: counts.active,
    dlq: dlqCounts.waiting + dlqCounts.completed
  };
}

function toTable(rows: WorkerSummary[]): string {
  const header = ['Worker', 'Success', 'Fail', 'Retry', 'DLQ', 'LockWait', 'OutOfOrder', 'AvgLat', 'P95Lat', 'P99Lat'];
  const data = rows.map((row) => [
    row.workerId,
    String(row.successCount),
    String(row.failCount),
    String(row.retryCount),
    String(row.dlqCount),
    String(row.lockContentionCount),
    String(row.outOfOrderCount),
    `${row.avgLatencyMs.toFixed(1)}ms`,
    `${row.p95LatencyMs.toFixed(1)}ms`,
    `${row.p99LatencyMs.toFixed(1)}ms`
  ]);

  const widths = header.map((name, i) =>
    Math.max(name.length, ...data.map((row) => row[i].length))
  );

  const makeRow = (cols: string[]): string =>
    `| ${cols.map((c, i) => c.padEnd(widths[i], ' ')).join(' | ')} |`;

  const separator = `|-${widths.map((w) => '-'.repeat(w)).join('-|-')}-|`;

  return [makeRow(header), separator, ...data.map(makeRow)].join('\n');
}

async function main(): Promise<void> {
  const [summaries, stats] = await Promise.all([loadWorkerMetrics(), loadQueueStats()]);

  const totalSuccess = summaries.reduce((sum, s) => sum + s.successCount, 0);
  const totalFail = summaries.reduce((sum, s) => sum + s.failCount, 0);
  const totalRetry = summaries.reduce((sum, s) => sum + s.retryCount, 0);
  const totalDlq = summaries.reduce((sum, s) => sum + s.dlqCount, 0);
  const totalLockWait = summaries.reduce((sum, s) => sum + s.lockContentionCount, 0);
  const totalOutOfOrder = summaries.reduce((sum, s) => sum + s.outOfOrderCount, 0);

  const table = toTable(summaries);

  const report = [
    '# Stress Test Report',
    `Generated: ${new Date().toISOString()}`,
    '',
    '## Global Queue Stats',
    `- Completed: ${stats.completed}`,
    `- Failed: ${stats.failed}`,
    `- Delayed (retrying): ${stats.delayed}`,
    `- Waiting: ${stats.waiting}`,
    `- Active: ${stats.active}`,
    `- DLQ: ${stats.dlq}`,
    '',
    '## Aggregated Worker Stats',
    `- Total Success: ${totalSuccess}`,
    `- Total Fail: ${totalFail}`,
    `- Total Retries: ${totalRetry}`,
    `- Total DLQ: ${totalDlq}`,
    `- Total Lock Contentions: ${totalLockWait}`,
    `- Total Out-of-Order Delays: ${totalOutOfOrder}`,
    '',
    '## Per-Worker Breakdown',
    table,
    ''
  ].join('\n');

  await writeFile(OUTPUT_FILE, report, 'utf8');

  console.log(report);
  console.log(`\nSaved report to: ${OUTPUT_FILE}`);
}

void main();
