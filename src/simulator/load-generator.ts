import { Kafka, Partitioners, logLevel } from 'kafkajs';
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
  const kafka = new Kafka({
    brokers: config.kafkaBrokers,
    clientId: `${config.kafkaClientId}-producer`,
    logLevel: logLevel.NOTHING
  });

  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();

  const events = buildMessages(args.conversations, args.messagesPerConversation);

  for (const event of events) {
    await producer.send({
      topic: config.topicMain,
      messages: [
        {
          key: event.conversationId,
          value: JSON.stringify(event)
        }
      ]
    });
  }

  console.log(
    `[producer] sent ${events.length} events | conversations=${args.conversations} | perConversation=${args.messagesPerConversation}`
  );

  await producer.disconnect();
}

void main();
