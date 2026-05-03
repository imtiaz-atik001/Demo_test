import { Injectable } from '@nestjs/common';
import Redis from 'ioredis';
import { config } from '../shared/config.js';

@Injectable()
export class SequenceTrackerService {
  private readonly redis: Redis;

  constructor() {
    this.redis = new Redis(config.redisUrl);
  }

  async onModuleDestroy(): Promise<void> {
    await this.redis.quit();
  }

  async getLastProcessedSequence(conversationId: string): Promise<number> {
    const val = await this.redis.get(`seq:conversation:${conversationId}`);
    return val ? parseInt(val, 10) : 0;
  }

  async setLastProcessedSequence(conversationId: string, sequence: number): Promise<void> {
    await this.redis.set(`seq:conversation:${conversationId}`, String(sequence));
  }
}
