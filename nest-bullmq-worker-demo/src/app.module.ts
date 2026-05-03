import { Module } from '@nestjs/common';
import { ConversationLockService } from './worker/conversation-lock.service.js';
import { SequenceTrackerService } from './worker/sequence-tracker.service.js';
import { MetricsService } from './worker/metrics.service.js';
import { JobProcessorService } from './worker/job-processor.service.js';
import { ConversationWorkerService } from './worker/conversation-worker.service.js';

@Module({
  providers: [
    ConversationLockService,
    SequenceTrackerService,
    MetricsService,
    JobProcessorService,
    ConversationWorkerService
  ]
})
export class AppModule {}
