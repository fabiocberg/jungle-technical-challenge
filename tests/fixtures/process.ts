import { loadConfig } from '../../src/infrastructure/config';
import { createDatabase } from '../../src/infrastructure/database';
import { SqsQueue } from '../../src/infrastructure/sqs';
import { Telemetry } from '../../src/infrastructure/telemetry';
import { WageringService } from '../../src/application/wagering';
import { WagerConsumer, OutboxPublisher } from '../../src/application/workers';
import { createHttpApp } from '../../src/http/app';

async function main() {
  const config = loadConfig(),
    db = await createDatabase(config).initialize(),
    queue = new SqsQueue(config);
  const telemetry = new Telemetry(),
    service = new WageringService(db, config, telemetry);
  const mode = process.argv[2];
  if (mode === 'http') {
    const app = await createHttpApp(service, queue, telemetry);
    await app.listen(0, '127.0.0.1');
    console.log(
      JSON.stringify({
        marker: 'READY',
        port: (app.getHttpServer().address() as { port: number }).port,
      }),
    );
    process.once('SIGTERM', () => {
      void app.close().then(async () => {
        queue.close();
        await db.destroy();
      });
    });
  } else if (mode === 'crash-consumer') {
    const consumer = new WagerConsumer(
      service,
      queue,
      config,
      telemetry,
      async () => {
        console.log(JSON.stringify({ marker: 'COMMITTED' }));
        await new Promise(() => {});
      },
    );
    console.log(JSON.stringify({ marker: 'READY' }));
    const [message] = await queue.receive();
    if (!message) throw new Error('EXPECTED_MESSAGE');
    await queue.changeVisibility(message.receiptHandle, 1);
    await consumer.handle(message);
  } else if (mode === 'publisher') {
    const publisher = new OutboxPublisher(db, queue, telemetry);
    console.log(JSON.stringify({ marker: 'READY' }));
    while (await publisher.tick()) {}
    queue.close();
    await db.destroy();
  } else {
    throw new Error('UNKNOWN_FIXTURE_MODE');
  }
}
void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
