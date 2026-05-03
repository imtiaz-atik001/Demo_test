export type ConversationMessage = {
  messageId: string;
  conversationId: string;
  sequence: number;
  payload: string;
  createdAt: string;
};

export type DlqEntry = ConversationMessage & {
  failedReason: string;
  failedAt: string;
  workerId: string;
};

export type WorkerConfig = {
  redisUrl: string;
  queueName: string;
  dlqName: string;
  processingDelayMs: number;
  maxRetries: number;
  workerConcurrency: number;
  lockTtlSeconds: number;
  workerId: string;
};
