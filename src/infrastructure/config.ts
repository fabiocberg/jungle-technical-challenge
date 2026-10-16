import { z } from 'zod';

const schema = z.object({
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  DATABASE_URL: z.url().default('postgres://wager:wager@localhost:5432/wager'),
  SQS_ENDPOINT: z.url().default('http://localhost:4566'),
  SQS_QUEUE_PREFIX: z
    .string()
    .max(24)
    .regex(/^[a-zA-Z0-9_-]*$/)
    .default(''),
  AWS_REGION: z.string().default('us-east-1'),
  AWS_ACCESS_KEY_ID: z.string().default('test'),
  AWS_SECRET_ACCESS_KEY: z.string().default('test'),
  WORKERS_ENABLED: z.enum(['true', 'false']).default('true'),
  SQS_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(100).default(5),
  REFERENCE_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(30).default(10),
  REFERENCE_BACKOFF_MS: z.coerce.number().int().min(1).default(1000),
  WORKER_INTERVAL_MS: z.coerce.number().int().min(10).default(500),
  DB_LOCK_TIMEOUT_MS: z.coerce.number().int().min(1).default(3000),
  SHUTDOWN_TIMEOUT_MS: z.coerce.number().int().min(100).default(20000),
});
export type Config = z.infer<typeof schema>;
export function loadConfig(env = process.env): Config {
  return schema.parse(env);
}
