import { Injectable, Logger } from '@nestjs/common';
import { appendFile, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { config } from '../shared/config.js';

export type WorkerMetrics = {
  successCount: number;
  failCount: number;
  retryCount: number;
  dlqCount: number;
  lockContentionCount: number;
  outOfOrderCount: number;
  latenciesMs: number[];
};

@Injectable()
export class MetricsService {
  private readonly logger = new Logger(MetricsService.name);
  private readonly metricsPath = join(process.cwd(), 'consumer-logs', `${this.toSafeFileName(config.workerId)}.jsonl`);

  private metrics: WorkerMetrics = {
    successCount: 0,
    failCount: 0,
    retryCount: 0,
    dlqCount: 0,
    lockContentionCount: 0,
    outOfOrderCount: 0,
    latenciesMs: []
  };

  private startTime = Date.now();
  private interval: NodeJS.Timeout | null = null;

  async startReporting(): Promise<void> {
    await mkdir(join(process.cwd(), 'consumer-logs'), { recursive: true });
    const header = JSON.stringify({ type: 'start', workerId: config.workerId, ts: new Date().toISOString() }) + '\n';
    await writeFile(this.metricsPath, header, 'utf8');

    this.interval = setInterval(() => {
      this.logPeriodicMetrics();
    }, 10000);
  }

  async stopReporting(): Promise<void> {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    await this.logPeriodicMetrics();
    const footer = JSON.stringify({ type: 'end', workerId: config.workerId, ts: new Date().toISOString() }) + '\n';
    await appendFile(this.metricsPath, footer, 'utf8');
  }

  recordSuccess(latencyMs: number): void {
    this.metrics.successCount++;
    this.metrics.latenciesMs.push(latencyMs);
  }

  recordFailure(): void {
    this.metrics.failCount++;
  }

  recordRetry(): void {
    this.metrics.retryCount++;
  }

  recordDlq(): void {
    this.metrics.dlqCount++;
  }

  recordLockContention(): void {
    this.metrics.lockContentionCount++;
  }

  recordOutOfOrder(): void {
    this.metrics.outOfOrderCount++;
  }

  getStats(): WorkerMetrics & {
    avgLatencyMs: number;
    p50LatencyMs: number;
    p95LatencyMs: number;
    p99LatencyMs: number;
    throughputPerSecond: number;
    elapsedSeconds: number;
  } {
    const elapsedSeconds = (Date.now() - this.startTime) / 1000;
    const sorted = [...this.metrics.latenciesMs].sort((a, b) => a - b);
    const totalProcessed = this.metrics.successCount + this.metrics.failCount;
    return {
      ...this.metrics,
      avgLatencyMs: sorted.length > 0 ? sorted.reduce((a, b) => a + b, 0) / sorted.length : 0,
      p50LatencyMs: this.percentile(sorted, 0.5),
      p95LatencyMs: this.percentile(sorted, 0.95),
      p99LatencyMs: this.percentile(sorted, 0.99),
      throughputPerSecond: elapsedSeconds > 0 ? totalProcessed / elapsedSeconds : 0,
      elapsedSeconds
    };
  }

  private percentile(sorted: number[], p: number): number {
    if (sorted.length === 0) return 0;
    const index = Math.ceil(sorted.length * p) - 1;
    return sorted[Math.max(0, index)];
  }

  private async logPeriodicMetrics(): Promise<void> {
    const stats = this.getStats();
    const logEntry = {
      type: 'metrics',
      workerId: config.workerId,
      ts: new Date().toISOString(),
      ...stats
    };
    await appendFile(this.metricsPath, JSON.stringify(logEntry) + '\n', 'utf8');

    this.logger.log(
      `stats success=${stats.successCount} fail=${stats.failCount} retry=${stats.retryCount} ` +
      `dlq=${stats.dlqCount} lockContention=${stats.lockContentionCount} outOfOrder=${stats.outOfOrderCount} ` +
      `p95=${stats.p95LatencyMs.toFixed(1)}ms throughput=${stats.throughputPerSecond.toFixed(2)}/s`
    );
  }

  private toSafeFileName(value: string): string {
    return value.replace(/[^a-zA-Z0-9-_]/g, '_');
  }
}
