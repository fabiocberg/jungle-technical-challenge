export interface QueueMessage {
  body: string;
  receiptHandle: string;
  receiveCount: number;
}
export interface QueuePort {
  receive(): Promise<QueueMessage[]>;
  acknowledge(receiptHandle: string): Promise<void>;
  changeVisibility(receiptHandle: string, seconds: number): Promise<void>;
  deadLetter(body: string, deduplicationId: string): Promise<void>;
  publish(body: string, aggregateId: string, eventId: string): Promise<void>;
  ready(): Promise<void>;
}

/** Replace with an OIDC JWT adapter binding authenticated provider to command.providerId. */
export abstract class ProviderIdentityPort {
  abstract assertProvider(
    providerId: string,
    bearerToken?: string,
  ): Promise<void>;
}
export class DevelopmentProviderIdentity extends ProviderIdentityPort {
  async assertProvider(
    _providerId: string,
    _bearerToken?: string,
  ): Promise<void> {}
}
