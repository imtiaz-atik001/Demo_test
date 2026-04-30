import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { appendFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import {
  Consumer,
  EachMessagePayload,
  Kafka,
  Producer,
  logLevel,
  Partitioners
} from 'kafkajs';
import { config } from '../shared/config.js';
import { ConversationMessage, RetryMeta } from '../shared/types.js';

const RETRY_DELAY_BY_TOPIC: Record<string, number> = {
  [config.topicRetryFast]: 1000,
  [config.topicRetrySlow]: 5000
};

@Injectable()
export class ConversationWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ConversationWorkerService.name);
  private readonly consumerLogPath = join(
    process.cwd(),
    'consumer-logs',
    `${this.toSafeFileName(config.workerId)}.txt`
  );
  private readonly kafka = new Kafka({
    brokers: config.kafkaBrokers,
    clientId: config.kafkaClientId,
    logLevel: logLevel.NOTHING
  });

  private readonly producer: Producer = this.kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner
  });

  private readonly consumer: Consumer = this.kafka.consumer({
    groupId: config.kafkaGroupId,
    heartbeatInterval: 3000,
    sessionTimeout: 30000
  });

  private readonly processedByConversation = new Map<string, number>();

  async onModuleInit(): Promise<void> {
    await this.ensureConsumerLogDirectory();
    await this.ensureTopics();

    await this.producer.connect();
    await this.consumer.connect();

    await this.consumer.subscribe({ topic: config.topicMain, fromBeginning: false });
    await this.consumer.subscribe({ topic: config.topicRetryFast, fromBeginning: false });
    await this.consumer.subscribe({ topic: config.topicRetrySlow, fromBeginning: false });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => {
        await this.handleMessage(payload);
      }
    });

    this.logger.log(`Worker ${config.workerId} connected. Group=${config.kafkaGroupId}`);
  }

  async onModuleDestroy(): Promise<void> {
    await this.consumer.disconnect().catch(() => undefined);
    await this.producer.disconnect().catch(() => undefined);
  }

  private async ensureTopics(): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();

    try {
      const topics = [
        { topic: config.topicMain, numPartitions: config.partitions },
        { topic: config.topicRetryFast, numPartitions: config.partitions },
        { topic: config.topicRetrySlow, numPartitions: config.partitions },
        { topic: config.topicDlq, numPartitions: config.partitions }
      ];

      await admin.createTopics({
        waitForLeaders: true,
        topics,
        timeout: 15000
      });

      const topicNames = topics.map((topicConfig) => topicConfig.topic);
      const metadata = await admin.fetchTopicMetadata({ topics: topicNames });

      const partitionsToIncrease = metadata.topics
        .map((topicMeta) => {
          const currentPartitions = topicMeta.partitions.length;
          if (currentPartitions >= config.partitions) {
            return undefined;
          }

          return {
            topic: topicMeta.name,
            count: config.partitions
          };
        })
        .filter(
          (topicConfig): topicConfig is { topic: string; count: number } => topicConfig !== undefined
        );

      if (partitionsToIncrease.length > 0) {
        await admin.createPartitions({
          topicPartitions: partitionsToIncrease,
          timeout: 15000
        });

        this.logger.warn(
          `Increased topic partitions to ${config.partitions} for: ${partitionsToIncrease
            .map((entry) => entry.topic)
            .join(', ')}`
        );
      }
    } finally {
      await admin.disconnect();
    }
  }

  private async handleMessage({ topic, partition, message, heartbeat }: EachMessagePayload): Promise<void> {
    if (!message.value) {
      await this.commitOffset(topic, partition, message.offset);
      return;
    }

    const event = JSON.parse(message.value.toString()) as ConversationMessage;
    const retryHeader = message.headers?.retryMeta;
    const retryMeta = this.parseRetryMeta(this.normalizeHeaderValue(retryHeader));

    await this.writeConsumerLog(
      `CONSUMED worker=${config.workerId} topic=${topic} partition=${partition} conv=${event.conversationId} seq=${event.sequence} offset=${message.offset} attempt=${retryMeta.attempt}`
    );

    const ownerInfo = `worker=${config.workerId} topic=${topic} partition=${partition} conv=${event.conversationId} seq=${event.sequence} offset=${message.offset}`;

    try {
      if (topic !== config.topicMain) {
        await this.waitForRetryDelay(topic);
      }

      await this.simulateConversationWrite(event);

      const total = (this.processedByConversation.get(event.conversationId) ?? 0) + 1;
      this.processedByConversation.set(event.conversationId, total);

      this.logger.log(`ACK ${ownerInfo} processedCount=${total} attempt=${retryMeta.attempt}`);
      await this.writeConsumerLog(
        `ACK worker=${config.workerId} topic=${topic} partition=${partition} conv=${event.conversationId} seq=${event.sequence} offset=${message.offset} attempt=${retryMeta.attempt}`
      );
      await this.commitOffset(topic, partition, message.offset);
      await heartbeat();
    } catch (error) {
      const err = error instanceof Error ? error : new Error('Unknown processing error');
      const nextAttempt = retryMeta.attempt + 1;

      if (nextAttempt > config.maxRetries) {
        await this.producer.send({
          topic: config.topicDlq,
          messages: [
            {
              key: event.conversationId,
              value: JSON.stringify(event),
              headers: {
                retryMeta: Buffer.from(
                  JSON.stringify({
                    ...retryMeta,
                    attempt: nextAttempt,
                    lastError: err.message
                  } satisfies RetryMeta)
                ),
                failedTopic: Buffer.from(topic)
              }
            }
          ]
        });

        this.logger.error(`DLQ ${ownerInfo} attempt=${nextAttempt} reason=${err.message}`);
        await this.writeConsumerLog(
          `DLQ worker=${config.workerId} topic=${topic} partition=${partition} conv=${event.conversationId} seq=${event.sequence} offset=${message.offset} attempt=${nextAttempt} reason=${err.message}`
        );
        await this.commitOffset(topic, partition, message.offset);
        return;
      }

      const retryTopic = nextAttempt <= 2 ? config.topicRetryFast : config.topicRetrySlow;

      await this.producer.send({
        topic: retryTopic,
        messages: [
          {
            key: event.conversationId,
            value: JSON.stringify(event),
            headers: {
              retryMeta: Buffer.from(
                JSON.stringify({
                  ...retryMeta,
                  attempt: nextAttempt,
                  lastError: err.message
                } satisfies RetryMeta)
              ),
              originalTopic: Buffer.from(topic)
            }
          }
        ]
      });

      this.logger.warn(`NACK ${ownerInfo} attempt=${nextAttempt} routedTo=${retryTopic} reason=${err.message}`);
      await this.writeConsumerLog(
        `NACK worker=${config.workerId} topic=${topic} partition=${partition} conv=${event.conversationId} seq=${event.sequence} offset=${message.offset} attempt=${nextAttempt} retryTopic=${retryTopic} reason=${err.message}`
      );
      await this.commitOffset(topic, partition, message.offset);
    }
  }

  private async ensureConsumerLogDirectory(): Promise<void> {
    await mkdir(join(process.cwd(), 'consumer-logs'), { recursive: true });
  }

  private async writeConsumerLog(entry: string): Promise<void> {
    const line = `${new Date().toISOString()} ${entry}\n`;
    await appendFile(this.consumerLogPath, line, 'utf8');
  }

  private toSafeFileName(value: string): string {
    return value.replace(/[^a-zA-Z0-9-_]/g, '_');
  }

  private parseRetryMeta(raw?: Buffer): RetryMeta {
    if (!raw) {
      return {
        attempt: 0,
        firstSeenAt: new Date().toISOString()
      };
    }

    try {
      return JSON.parse(raw.toString()) as RetryMeta;
    } catch {
      return {
        attempt: 0,
        firstSeenAt: new Date().toISOString()
      };
    }
  }

  private normalizeHeaderValue(
    value?: string | Buffer | Array<string | Buffer>
  ): Buffer | undefined {
    if (!value) {
      return undefined;
    }

    if (Array.isArray(value)) {
      const first = value[0];
      return typeof first === 'string' ? Buffer.from(first) : first;
    }

    return typeof value === 'string' ? Buffer.from(value) : value;
  }

  private async simulateConversationWrite(event: ConversationMessage): Promise<void> {
    await this.sleep(config.processingDelayMs);

    // Simulate occasional transient errors so retry and DLQ flow can be observed under stress.
    const transientFailure = event.sequence % 17 === 0;
    if (transientFailure) {
      throw new Error(`Transient DB timeout for conversation=${event.conversationId} seq=${event.sequence}`);
    }
  }

  private async waitForRetryDelay(topic: string): Promise<void> {
    const delay = RETRY_DELAY_BY_TOPIC[topic] ?? 0;
    if (delay > 0) {
      await this.sleep(delay);
    }
  }

  private async commitOffset(topic: string, partition: number, currentOffset: string): Promise<void> {
    const nextOffset = (BigInt(currentOffset) + 1n).toString();
    await this.consumer.commitOffsets([{ topic, partition, offset: nextOffset }]);
  }

  private async sleep(ms: number): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}
