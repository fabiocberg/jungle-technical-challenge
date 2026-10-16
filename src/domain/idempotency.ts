import { createHash } from 'node:crypto';
import { DomainError } from './errors';

export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object)
      .filter((key) => object[key] !== undefined)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}
export function payloadHash(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}
export function assertReplay(storedHash: string, incomingHash: string): void {
  if (storedHash !== incomingHash)
    throw new DomainError('IDEMPOTENCY_CONFLICT');
}
