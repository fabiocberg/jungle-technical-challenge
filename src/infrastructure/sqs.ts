import {
  SQSClient,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import type { QueueMessage, QueuePort } from '../application/ports';
import type { Config } from './config';

export const QUEUES = {
  requests: 'wager-transactions.fifo',
  deadLetters: 'wager-transactions-dlq.fifo',
  events: 'wager-events.fifo',
} as const;
export class SqsQueue implements QueuePort {
  readonly client: SQSClient;
  private readonly urls = new Map<string, string>();
  constructor(private readonly config: Config) {
    this.client = new SQSClient({
      endpoint: config.SQS_ENDPOINT,
      region: config.AWS_REGION,
      credentials: {
        accessKeyId: config.AWS_ACCESS_KEY_ID,
        secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
      },
      maxAttempts: 2,
      requestHandler: {
        requestTimeout: 5000,
        connectionTimeout: 2000,
        throwOnRequestTimeout: true,
      },
    });
  }
  async url(name: string): Promise<string> {
    const cached = this.urls.get(name);
    if (cached) return cached;
    const result = await this.client.send(
      new GetQueueUrlCommand({
        QueueName: this.config.SQS_QUEUE_PREFIX + name,
      }),
    );
    if (!result.QueueUrl) throw new Error('QUEUE_NOT_FOUND');
    // LocalStack advertises URLs using the caller's host; keep them usable inside Compose too.
    const url = new URL(result.QueueUrl);
    const endpoint = new URL(this.config.SQS_ENDPOINT);
    url.host = endpoint.host;
    url.protocol = endpoint.protocol;
    this.urls.set(name, url.toString());
    return url.toString();
  }
  async receive(): Promise<QueueMessage[]> {
    const result = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: await this.url(QUEUES.requests),
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 1,
        VisibilityTimeout: 30,
        MessageSystemAttributeNames: ['ApproximateReceiveCount'],
      }),
    );
    return (result.Messages ?? [])
      .filter((m) => m.Body && m.ReceiptHandle)
      .map((m) => ({
        body: m.Body!,
        receiptHandle: m.ReceiptHandle!,
        receiveCount: Number(m.Attributes?.ApproximateReceiveCount ?? '1'),
      }));
  }
  async acknowledge(receiptHandle: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: await this.url(QUEUES.requests),
        ReceiptHandle: receiptHandle,
      }),
    );
  }
  async changeVisibility(
    receiptHandle: string,
    seconds: number,
  ): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: await this.url(QUEUES.requests),
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: seconds,
      }),
    );
  }
  async deadLetter(body: string, deduplicationId: string): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: await this.url(QUEUES.deadLetters),
        MessageBody: body,
        MessageGroupId: 'dead-letters',
        MessageDeduplicationId: deduplicationId,
      }),
    );
  }
  async publish(
    body: string,
    aggregateId: string,
    eventId: string,
  ): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: await this.url(QUEUES.events),
        MessageBody: body,
        MessageGroupId: aggregateId,
        MessageDeduplicationId: eventId,
      }),
    );
  }
  async ready(): Promise<void> {
    await Promise.all(
      Object.values(QUEUES).map(async (name) =>
        this.client.send(
          new GetQueueAttributesCommand({
            QueueUrl: await this.url(name),
            AttributeNames: ['QueueArn'],
          }),
        ),
      ),
    );
  }
  close(): void {
    this.client.destroy();
  }
}
