import { loadConfig } from './infrastructure/config';
import { createDatabase } from './infrastructure/database';
import { Telemetry } from './infrastructure/telemetry';
import { SqsQueue } from './infrastructure/sqs';
import { WageringService } from './application/wagering';
import {
  WagerConsumer,
  OutboxPublisher,
  ReferenceWorker,
  WorkerSupervisor,
} from './application/workers';
import { createHttpApp } from './http/app';

async function main() {
  const config = loadConfig();
  const telemetry = new Telemetry();
  const db = await createDatabase(config).initialize();
  const queue = new SqsQueue(config);
  const service = new WageringService(db, config, telemetry);
  const app = await createHttpApp(service, queue, telemetry);
  const workers = new WorkerSupervisor(
    new WagerConsumer(service, queue, config, telemetry),
    new OutboxPublisher(db, queue, telemetry),
    new ReferenceWorker(service),
    config,
    telemetry,
  );
  let closing = false;
  const close = async () => {
    if (closing) return;
    closing = true;
    telemetry.log('shutdown_started');
    const httpClosed = app.close();
    await workers.stop();
    await httpClosed;
    queue.close();
    await db.destroy();
    telemetry.log('shutdown_complete');
  };
  process.once('SIGTERM', () => {
    void close().catch(() => {
      process.exitCode = 1;
    });
  });
  process.once('SIGINT', () => {
    void close().catch(() => {
      process.exitCode = 1;
    });
  });
  try {
    await app.listen(config.PORT, '0.0.0.0');
  } catch (error) {
    await close();
    throw error;
  }
  if (config.WORKERS_ENABLED === 'true') workers.start();
  telemetry.log('service_started');
}
main().catch(() => {
  console.error(
    JSON.stringify({
      event: 'startup_failed',
      code: 'CONFIGURATION_OR_DEPENDENCY_ERROR',
    }),
  );
  process.exitCode = 1;
});
