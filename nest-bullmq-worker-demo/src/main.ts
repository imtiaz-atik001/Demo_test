import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ['log', 'warn', 'error']
  });

  const logger = new Logger('Bootstrap');
  logger.log('Standalone BullMQ worker started. Press Ctrl+C to stop.');

  process.on('SIGINT', async () => {
    logger.warn('SIGINT received. Closing worker context.');
    await app.close();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    logger.warn('SIGTERM received. Closing worker context.');
    await app.close();
    process.exit(0);
  });
}

void bootstrap();
