import 'reflect-metadata';
import { DataSource, EntitySchema } from 'typeorm';
import type { Config } from './config';
import { InitialSchema1792100000000 } from './migrations/1792100000000-InitialSchema';

export interface WalletRecord {
  id: string;
  playerId: string;
  currency: string;
  balance: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}
export const WalletSchema = new EntitySchema<WalletRecord>({
  name: 'WalletRecord',
  tableName: 'wallets',
  columns: {
    id: { type: 'uuid', primary: true },
    playerId: { name: 'player_id', type: 'uuid' },
    currency: { type: 'text' },
    balance: { type: 'numeric', precision: 28, scale: 2 },
    version: { type: 'integer' },
    createdAt: { name: 'created_at', type: 'timestamptz' },
    updatedAt: { name: 'updated_at', type: 'timestamptz' },
  },
});
export function createDatabase(config: Config): DataSource {
  return new DataSource({
    type: 'postgres',
    url: config.DATABASE_URL,
    entities: [WalletSchema],
    migrations: [InitialSchema1792100000000],
    synchronize: false,
    logging: false,
    extra: {
      max: 12,
      connectionTimeoutMillis: 3000,
      statement_timeout: 10000,
      application_name: 'jungle-wager',
    },
  });
}
export function databaseCode(error: unknown): string | undefined {
  if (typeof error === 'object' && error !== null && 'code' in error)
    return String(error.code);
  return undefined;
}
