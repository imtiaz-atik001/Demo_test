const workerIdArg = process.argv[2];

if (workerIdArg && workerIdArg.trim().length > 0) {
  process.env.WORKER_ID = workerIdArg.trim();
}

async function run(): Promise<void> {
  await import('../main.js');
}

void run();
