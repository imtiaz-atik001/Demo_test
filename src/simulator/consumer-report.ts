import { readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

type ConsumerSummary = {
  consumer: string;
  consumedCount: number;
  conversations: string[];
};

const LOG_DIR = join(process.cwd(), 'consumer-logs');
const OUTPUT_FILE = join(LOG_DIR, 'summary-table.txt');

function parseLineForConversation(line: string): string | undefined {
  const match = line.match(/\bconv=([^\s]+)/);
  return match?.[1];
}

function isConsumedLine(line: string): boolean {
  return line.includes(' CONSUMED ');
}

function toTable(rows: ConsumerSummary[]): string {
  const header = ['Consumer', 'Consumed Messages', 'Conversations'];
  const data = rows.map((row) => [
    row.consumer,
    String(row.consumedCount),
    row.conversations.length > 0 ? row.conversations.join(', ') : '-'
  ]);

  const widths = header.map((name, i) =>
    Math.max(name.length, ...data.map((row) => row[i].length))
  );

  const makeRow = (cols: string[]): string =>
    `| ${cols.map((c, i) => c.padEnd(widths[i], ' ')).join(' | ')} |`;

  const separator = `|-${widths.map((w) => '-'.repeat(w)).join('-|-')}-|`;

  return [makeRow(header), separator, ...data.map(makeRow)].join('\n');
}

async function main(): Promise<void> {
  const files = await readdir(LOG_DIR).catch(() => [] as string[]);
  const workerFiles = files
    .filter((file) => file.endsWith('.txt'))
    .filter((file) => file !== 'summary-table.txt')
    .sort((a, b) => a.localeCompare(b));

  const summaries: ConsumerSummary[] = [];

  for (const file of workerFiles) {
    const absolutePath = join(LOG_DIR, file);
    const content = await readFile(absolutePath, 'utf8');
    const lines = content.split(/\r?\n/).filter((line) => line.trim().length > 0);

    const consumedLines = lines.filter(isConsumedLine);
    const conversations = new Set<string>();

    for (const line of consumedLines) {
      const conversation = parseLineForConversation(line);
      if (conversation) {
        conversations.add(conversation);
      }
    }

    summaries.push({
      consumer: file.replace(/\.txt$/, ''),
      consumedCount: consumedLines.length,
      conversations: [...conversations].sort((a, b) => a.localeCompare(b))
    });
  }

  const table = toTable(summaries);
  await writeFile(OUTPUT_FILE, `${table}\n`, 'utf8');

  console.log(table);
  console.log(`\nSaved summary to: ${OUTPUT_FILE}`);
}

void main();
