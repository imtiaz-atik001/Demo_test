import { Injectable, Logger } from '@nestjs/common';
import Redis from 'ioredis';
import { config } from '../shared/config.js';

@Injectable()
export class ConversationLockService {
  private readonly logger = new Logger(ConversationLockService.name);
  private readonly redis: Redis;

  constructor() {
    this.redis = new Redis(config.redisUrl);
  }

  async onModuleDestroy(): Promise<void> {
    await this.redis.quit();
  }

  async acquireLock(conversationId: string, workerId: string): Promise<boolean> {
    const lockKey = `lock:conversation:${conversationId}`;
    const result = await this.redis.set(lockKey, workerId, 'EX', config.lockTtlSeconds, 'NX');
    const acquired = result === 'OK';
    if (acquired) {
      this.logger.debug(`Lock acquired for ${conversationId} by ${workerId}`);
    }
    return acquired;
  }

  async releaseLock(conversationId: string, workerId: string): Promise<void> {
    const lockKey = `lock:conversation:${conversationId}`;
    const currentOwner = await this.redis.get(lockKey);
    if (currentOwner === workerId) {
      await this.redis.del(lockKey);
      this.logger.debug(`Lock released for ${conversationId} by ${workerId}`);
    } else if (currentOwner) {
      this.logger.warn(`Lock for ${conversationId} owned by ${currentOwner}, cannot release by ${workerId}`);
    }
  }

  async extendLock(conversationId: string, workerId: string): Promise<void> {
    const lockKey = `lock:conversation:${conversationId}`;
    const currentOwner = await this.redis.get(lockKey);
    if (currentOwner === workerId) {
      await this.redis.expire(lockKey, config.lockTtlSeconds);
    }
  }

  async getLockOwner(conversationId: string): Promise<string | null> {
    return this.redis.get(`lock:conversation:${conversationId}`);
  }
}
