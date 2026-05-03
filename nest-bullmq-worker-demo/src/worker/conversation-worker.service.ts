import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Job, Queue, Worker } from 'bullmq';
import Redis from 'ioredis';
import { config } from '../shared/config.js';
import { ConversationMessage, DlqEntry } from '../shared/types.js';
import { ConversationLockService } from './conversation-lock.service.js';
import { SequenceTrackerService } from './sequence-tracker.service.js';
import { MetricsService } from './metrics.service.js';
import { JobProcessorService, LockContentionError, OutOfOrderError } from './job-processor.service.js';

@Injectable()
export class ConversationWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ConversationWorkerService.name);
  private readonly redisConnection: Redis;
  private queue!: Queue<ConversationMessage>;
  private dlqQueue!: Queue<DlqEntry>;
  private worker!: Worker<ConversationMessage>;
  private isShuttingDown = false;

  constructor(
    private readonly lockService: ConversationLockService,
    private readonly sequenceTracker: SequenceTrackerService,
    private readonly metrics: MetricsService,
    private readonly processor: JobProcessorService
  ) {
    this.redisConnection = new Redis(config.redisUrl, { maxRetriesPerRequest: null });
  }

  async onModuleInit(): Promise<void> {
    this.queue = new Queue<ConversationMessage>(config.queueName, {
      connection: this.redisConnection,
      defaultJobOptions: {
        attempts: config.maxRetries,
        backoff: {
          type: 'exponential',
          delay: 1000
        },
        removeOnComplete: { count: 500 },
        removeOnFail: { count: 500 }
      }
    });

    this.dlqQueue = new Queue<DlqEntry>(config.dlqName, {
      connection: this.redisConnection
    });

    this.worker = new Worker<ConversationMessage>(
      config.queueName,
      async (job) => this.processor.process(job),
      {
        connection: this.redisConnection,
        concurrency: config.workerConcurrency,
        drainDelay: 500
      }
    );

    this.worker.on('completed', (job) => {
      this.logger.debug(`Job completed: ${job.id}`);
    });

    this.worker.on('failed', async (job, err) => {
      if (!job) return;
      this.metrics.recordRetry();

      if (job.attemptsMade >= (job.opts.attempts ?? config.maxRetries)) {
        this.metrics.recordDlq();
        await this.moveToDlq(job, err);
      }
    });

    await this.metrics.startReporting();

    const initialCounts = await this.queue.getJobCounts('waiting', 'active', 'delayed', 'completed', 'failed');
    this.logger.log(`Worker ${config.workerId} started. concurrency=${config.workerConcurrency} maxRetries=${config.maxRetries}`);
    this.logger.log(`Queue ${config.queueName} state: waiting=${initialCounts.waiting} active=${initialCounts.active} delayed=${initialCounts.delayed} completed=${initialCounts.completed} failed=${initialCounts.failed}`);

    // Periodic health log
    setInterval(() => {
      this.logHealth();
    }, 15000);
  }

  async onModuleDestroy(): Promise<void> {
    this.isShuttingDown = true;
    this.logger.warn('Shutting down worker...');

    await this.worker.close();
    await this.queue.close();
    await this.dlqQueue.close();
    await this.metrics.stopReporting();
    await this.redisConnection.quit();

    this.logger.log('Worker shut down gracefully.');
  }

  private async moveToDlq(job: Job<ConversationMessage>, err: Error): Promise<void> {
    const event = job.data;
    const dlqEntry: DlqEntry = {
      ...event,
      failedReason: err.message,
      failedAt: new Date().toISOString(),
      workerId: config.workerId
    };

    await this.dlqQueue.add('failed', dlqEntry, {
      jobId: `dlq:${job.id}`
    });

    this.logger.error(`DLQ job=${job.id} conv=${event.conversationId} seq=${event.sequence} reason=${err.message}`);
  }

  private async logHealth(): Promise<void> {
    if (this.isShuttingDown) return;

    const counts = await this.queue.getJobCounts('waiting', 'active', 'delayed', 'completed', 'failed');
    const dlqCounts = await this.dlqQueue.getJobCounts('waiting', 'completed');

    this.logger.log(
      `health worker=${config.workerId} waiting=${counts.waiting} active=${counts.active} ` +
      `delayed=${counts.delayed} completed=${counts.completed} failed=${counts.failed} dlq=${dlqCounts.waiting + dlqCounts.completed}`
    );
  }
}
