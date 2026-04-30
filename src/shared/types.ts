export type ConversationMessage = {
  messageId: string;
  conversationId: string;
  sequence: number;
  payload: string;
  createdAt: string;
};

export type RetryMeta = {
  attempt: number;
  firstSeenAt: string;
  lastError?: string;
};

export type WorkerConfig = {
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaGroupId: string;
  topicMain: string;
  topicRetryFast: string;
  topicRetrySlow: string;
  topicDlq: string;
  partitions: number;
  processingDelayMs: number;
  maxRetries: number;
  workerId: string;
};
