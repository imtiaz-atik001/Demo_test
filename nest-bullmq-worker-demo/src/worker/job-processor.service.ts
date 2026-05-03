import { Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { config } from '../shared/config.js';
import { ConversationMessage } from '../shared/types.js';
import { ConversationLockService } from './conversation-lock.service.js';
import { SequenceTrackerService } from './sequence-tracker.service.js';
import { MetricsService } from './metrics.service.js';

export class LockContentionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'LockContentionError';
  }
}

export class OutOfOrderError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'OutOfOrderError';
  }
}

export class TransientError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TransientError';
  }
}

@Injectable()
export class JobProcessorService {
  private readonly logger = new Logger(JobProcessorService.name);

  constructor(
    private readonly lockService: ConversationLockService,
    private readonly sequenceTracker: SequenceTrackerService,
    private readonly metrics: MetricsService
  ) {}

  async process(job: Job<ConversationMessage>): Promise<void> {
    const event = job.data;
    const startTime = Date.now();
    const ownerInfo = `worker=${config.workerId} job=${job.id} conv=${event.conversationId} seq=${event.sequence} attempt=${job.attemptsMade + 1}`;

    // 1. Ordering check
    const lastSeq = await this.sequenceTracker.getLastProcessedSequence(event.conversationId);
    if (event.sequence <= lastSeq) {
      // Idempotent: already processed
      this.logger.debug(`SKIP already-processed ${ownerInfo} lastSeq=${lastSeq}`);
      return;
    }
    if (event.sequence > lastSeq + 1) {
      this.metrics.recordOutOfOrder();
      throw new OutOfOrderError(`Expected seq ${lastSeq + 1}, got ${event.sequence} for ${event.conversationId}`);
    }

    // 2. Acquire distributed lock for this conversation
    const acquired = await this.lockService.acquireLock(event.conversationId, config.workerId);
    if (!acquired) {
      const owner = await this.lockService.getLockOwner(event.conversationId);
      this.metrics.recordLockContention();
      throw new LockContentionError(`Conversation ${event.conversationId} locked by ${owner}`);
    }

    // Start lock extension heartbeat
    const lockInterval = setInterval(async () => {
      await this.lockService.extendLock(event.conversationId, config.workerId);
    }, (config.lockTtlSeconds / 2) * 1000);

    try {
      this.logger.log(`START ${ownerInfo}`);

      // 3. Simulate processing
      await this.simulateConversationWrite(event);

      // 4. Update sequence tracker
      await this.sequenceTracker.setLastProcessedSequence(event.conversationId, event.sequence);

      const latencyMs = Date.now() - startTime;
      this.metrics.recordSuccess(latencyMs);
      this.logger.log(`ACK ${ownerInfo} latency=${latencyMs}ms`);
    } catch (error) {
      const err = error instanceof Error ? error : new Error('Unknown processing error');
      this.metrics.recordFailure();
      this.logger.error(`NACK ${ownerInfo} reason=${err.message}`);
      throw err;
    } finally {
      clearInterval(lockInterval);
      await this.lockService.releaseLock(event.conversationId, config.workerId);
    }
  }

  private async simulateConversationWrite(event: ConversationMessage): Promise<void> {
    await this.sleep(config.processingDelayMs);

    // Simulate occasional transient errors (every 17th message)
    if (event.sequence % 17 === 0) {
      throw new TransientError(`Transient DB timeout for conversation=${event.conversationId} seq=${event.sequence}`);
    }
  }

  private async sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
