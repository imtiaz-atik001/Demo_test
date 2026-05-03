import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { config } from '../shared/config.js';
import { ConversationMessage } from '../shared/types.js';

type ProduceArgs = {
  conversations: number;
  messagesPerConversation: number;
};

function parseArgs(): ProduceArgs {
  const conversations = Number(process.argv[2] ?? '5');
  const messagesPerConversation = Number(process.argv[3] ?? '40');
  return {
    conversations: Number.isFinite(conversations) ? conversations : 5,
    messagesPerConversation: Number.isFinite(messagesPerConversation) ? messagesPerConversation : 40
  };
}

function buildMessages(conversations: number, messagesPerConversation: number): ConversationMessage[] {
  const events: ConversationMessage[] = [];
  for (let c = 1; c <= conversations; c += 1) {
    const conversationId = `conv-${c}`;
    for (let s = 1; s <= messagesPerConversation; s += 1) {
      events.push({
        messageId: `${conversationId}-${s}`,
        conversationId,
        sequence: s,
        payload: `message-${s}`,
        createdAt: new Date().toISOString()
      });
    }
  }
  return events;
}

async function main(): Promise<void> {
  const args = parseArgs();
  const redisConnection = new Redis(config.redisUrl);
  const queue = new Queue<ConversationMessage>(config.queueName, { connection: redisConnection });

  const existingCounts = await queue.getJobCounts('waiting', 'active', 'delayed', 'completed', 'failed');
  const existingTotal = existingCounts.waiting + existingCounts.active + existingCounts.delayed;
  if (existingTotal > 0) {
    console.log(`[producer] queue has ${existingTotal} pending/active jobs; adding new jobs alongside them`);
  }

  const events = buildMessages(args.conversations, args.messagesPerConversation);

  for (const event of events) {
    await queue.add('process', event, {
      jobId: `${event.conversationId}-${event.sequence}`
    });
  }

  console.log(
    `[producer] sent ${events.length} jobs | conversations=${args.conversations} | perConversation=${args.messagesPerConversation}`
  );

  await queue.close();
  await redisConnection.quit();
}

void main();
