const workerIdArg = process.argv[2];

if (workerIdArg && workerIdArg.trim().length > 0) {
  process.env.WORKER_ID = workerIdArg.trim();
}

await import('../main.js');
