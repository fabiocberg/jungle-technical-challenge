import { createDatabase } from '../src/infrastructure/database';
import { loadConfig } from '../src/infrastructure/config';

async function main() {
  const db = await createDatabase(loadConfig()).initialize();
  try {
    // Migrations are a single deployment step, serialized independently of wallet processing.
    const runner = db.createQueryRunner();
    await runner.connect();
    try {
      await runner.query('SELECT pg_advisory_lock(1792100000)');
      if (process.argv[2] === 'down') await db.undoLastMigration();
      else
        console.log(
          (await db.runMigrations({ transaction: 'all' })).map(
            (migration) => migration.name,
          ),
        );
    } finally {
      await runner.query('SELECT pg_advisory_unlock(1792100000)');
      await runner.release();
    }
  } finally {
    await db.destroy();
  }
}
main().catch(() => {
  console.error('Migration failed; inspect database diagnostics.');
  process.exitCode = 1;
});
