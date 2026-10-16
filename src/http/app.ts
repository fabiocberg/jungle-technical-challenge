import 'reflect-metadata';
import {
  Catch,
  Controller,
  Get,
  Post,
  Param,
  Body,
  Query,
  Headers,
  Req,
  Res,
  Inject,
  Module,
  type ExceptionFilter,
  type ArgumentsHost,
  HttpException,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { Request, Response } from 'express';
import { ZodError } from 'zod';
import type { DataSource } from 'typeorm';
import { WageringService } from '../application/wagering';
import { QueryService } from '../application/queries';
import {
  DevelopmentProviderIdentity,
  ProviderIdentityPort,
  type QueuePort,
} from '../application/ports';
import { ApplicationError, DomainError } from '../domain/errors';
import { databaseCode } from '../infrastructure/database';
import { Telemetry } from '../infrastructure/telemetry';

interface CorrelatedRequest extends Request {
  correlationId: string;
}
const DATABASE = Symbol('database'),
  QUEUE = Symbol('queue');

@Catch()
class ApiErrors implements ExceptionFilter {
  constructor(private readonly telemetry: Telemetry) {}
  catch(error: unknown, host: ArgumentsHost): void {
    const req = host.switchToHttp().getRequest<CorrelatedRequest>();
    const res = host.switchToHttp().getResponse<Response>();
    let status = 500,
      code = 'INTERNAL_ERROR';
    if (error instanceof ZodError) {
      status = 400;
      code = 'INVALID_PAYLOAD';
    } else if (error instanceof ApplicationError) {
      status = error.httpStatus;
      code = error.code;
    } else if (error instanceof DomainError) {
      status = error.code === 'IDEMPOTENCY_CONFLICT' ? 409 : 400;
      code = error.code;
    } else if (error instanceof HttpException) {
      status = error.getStatus();
      code = status === 400 ? 'INVALID_PAYLOAD' : 'HTTP_ERROR';
    } else if (
      databaseCode(error) ||
      (error instanceof Error &&
        /connect|timeout|closed|ECONN/i.test(error.message))
    ) {
      status = 503;
      code = 'INFRASTRUCTURE_UNAVAILABLE';
    }
    if (status >= 500)
      this.telemetry.log('request_failed', {
        correlationId: req.correlationId,
        code,
      });
    res.status(status).json({ code, correlationId: req.correlationId });
  }
}

@Controller()
class ApiController {
  constructor(
    @Inject(WageringService) private readonly service: WageringService,
    @Inject(QueryService) private readonly queries: QueryService,
    @Inject(DATABASE) private readonly db: DataSource,
    @Inject(QUEUE) private readonly queue: QueuePort,
    @Inject(Telemetry) private readonly telemetry: Telemetry,
    @Inject(ProviderIdentityPort)
    private readonly identity: ProviderIdentityPort,
  ) {}
  @Post('wallets')
  open(@Body() body: unknown, @Req() req: CorrelatedRequest) {
    return this.service.openWallet(body, { correlationId: req.correlationId });
  }
  @Get('wallets/:walletId')
  wallet(@Param('walletId') id: string) {
    return this.queries.wallet(id);
  }
  @Get('wallets/:walletId/ledger')
  ledger(@Param('walletId') id: string, @Query() query: unknown) {
    return this.queries.ledger(id, query);
  }
  @Post('wallets/:walletId/reconciliation')
  reconciliation(
    @Param('walletId') id: string,
    @Req() req: CorrelatedRequest,
    @Res({ passthrough: true }) res: Response,
  ) {
    res.status(200);
    return this.queries.reconcile(id, req.correlationId);
  }
  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  external(
    @Param('providerId') provider: string,
    @Param('externalTransactionId') external: string,
  ) {
    return this.queries.externalTransaction(provider, external);
  }
  @Get('wagering/transactions/:transactionId')
  transaction(@Param('transactionId') id: string) {
    return this.queries.transaction(id);
  }
  @Post('wagering/transactions')
  async submit(
    @Body() body: unknown,
    @Headers('idempotency-key') key: unknown,
    @Headers('authorization') authorization: string | undefined,
    @Req() req: CorrelatedRequest,
    @Res({ passthrough: true }) res: Response,
  ) {
    if (
      body &&
      typeof body === 'object' &&
      'providerId' in body &&
      typeof body.providerId === 'string'
    )
      await this.identity.assertProvider(body.providerId, authorization);
    const result = await this.service.process(body, key, {
      correlationId: req.correlationId,
    });
    res.status(
      result.status === 'PENDING_REFERENCE'
        ? 202
        : result.status === 'REJECTED'
          ? 422
          : result.status === 'FAILED'
            ? 503
            : 200,
    );
    return result;
  }
  @Get('health/live') live() {
    return { status: 'live' };
  }
  @Get('health/ready') async ready() {
    try {
      await Promise.all([this.db.query('SELECT 1'), this.queue.ready()]);
      return { status: 'ready' };
    } catch {
      throw new ApplicationError('DEPENDENCIES_UNAVAILABLE', 503);
    }
  }
  @Get('metrics') async metrics(@Res() res: Response) {
    res
      .type(this.telemetry.registry.contentType)
      .send(await this.telemetry.registry.metrics());
  }
}

export async function createHttpApp(
  service: WageringService,
  queue: QueuePort,
  telemetry: Telemetry,
) {
  @Module({
    controllers: [ApiController],
    providers: [
      { provide: WageringService, useValue: service },
      {
        provide: QueryService,
        useValue: new QueryService(service.db, telemetry),
      },
      { provide: DATABASE, useValue: service.db },
      { provide: QUEUE, useValue: queue },
      { provide: Telemetry, useValue: telemetry },
      { provide: ProviderIdentityPort, useClass: DevelopmentProviderIdentity },
    ],
  })
  class ApiModule {}
  const app = await NestFactory.create(ApiModule, {
    logger: false,
    bodyParser: true,
  });
  app.use((req: CorrelatedRequest, res: Response, next: () => void) => {
    const incoming = req.header('x-correlation-id');
    req.correlationId =
      incoming && /^[a-zA-Z0-9:._-]{1,128}$/.test(incoming)
        ? incoming
        : crypto.randomUUID();
    res.setHeader('X-Correlation-Id', req.correlationId);
    next();
  });
  app.useGlobalFilters(new ApiErrors(telemetry));
  return app;
}
