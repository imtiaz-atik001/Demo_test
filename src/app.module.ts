import { Module } from '@nestjs/common';
import { ConversationWorkerService } from './worker/conversation-worker.service.js';

@Module({
  providers: [ConversationWorkerService]
})
export class AppModule {}
