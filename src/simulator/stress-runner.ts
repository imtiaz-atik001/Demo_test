import { spawn } from 'node:child_process';

type StressConfig = {
  workers: number;
  conversations: number;
  messagesPerConversation: number;
};

function parseArgs(): StressConfig {
  return {
    workers: Number(process.argv[2] ?? '3'),
    conversations: Number(process.argv[3] ?? '30'),
    messagesPerConversation: Number(process.argv[4] ?? '200')
  };
}

async function run(command: string, args: string[], label: string): Promise<void> {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: 'inherit', shell: true });

    child.on('exit', (code) => {
      if (code === 0) {
        resolve(undefined);
      } else {
        reject(new Error(`${label} failed with code=${code}`));
      }
    });

    child.on('error', reject);
  });
}

async function main(): Promise<void> {
  const cfg = parseArgs();

  console.log(`[stress] starting ${cfg.workers} worker processes`);

  const workers: ReturnType<typeof spawn>[] = [];
  for (let i = 1; i <= cfg.workers; i += 1) {
    workers.push(
      spawn('npm', ['run', `worker:${Math.min(i, 3)}`], {
        stdio: 'inherit',
        shell: true,
        env: { ...process.env, WORKER_ID: `worker-${i}` }
      })
    );
  }

  await new Promise((r) => setTimeout(r, 3000));

  await run(
    'npm',
    ['run', 'simulate:produce', '--', String(cfg.conversations), String(cfg.messagesPerConversation)],
    'load-generator'
  );

  console.log('[stress] load sent. Let workers process for 20 seconds...');
  await new Promise((r) => setTimeout(r, 20000));

  for (const worker of workers) {
    worker.kill('SIGINT');
  }

  await run('npm', ['run', 'report:consumers'], 'consumer-report');

  console.log('[stress] completed');
}

void main();
