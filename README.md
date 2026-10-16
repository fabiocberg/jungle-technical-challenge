# Jungle · Wagering Processor

Processador de transações de jogos com saldo exato, ledger auditável e processamento seguro diante de concorrência, duplicação de mensagens e reinícios.

Implementação do [Backend Challenge da Jungle](https://github.com/junglegaming/backend-challenge), com **Bun 1.4.3, TypeScript estrito, NestJS, TypeORM, PostgreSQL 17 e SQS via LocalStack**. A API HTTP e o consumidor SQS executam o mesmo caso de uso.

## Execução rápida

Pré-requisitos: Docker com Compose v2+ e daemon ativo. Bun 1.4.3 é necessário somente para executar os comandos fora dos containers. Em macOS, Docker Desktop ou Colima atendem ao requisito.

```sh
docker compose up -d --build --wait
curl --fail http://localhost:3000/health/ready
```

O Compose inicia PostgreSQL, provisiona as três filas, executa as migrations e sobe a aplicação. Não é necessário configurar conta AWS. As credenciais `test` e `wager` são exclusivas do ambiente local.

| Serviço             | Endereço                                      |
| ------------------- | --------------------------------------------- |
| API                 | `http://localhost:3000`                       |
| PostgreSQL          | `localhost:5432`, banco/usuário/senha `wager` |
| LocalStack SQS      | `http://localhost:4566`                       |
| Métricas Prometheus | `http://localhost:3000/metrics`               |

Para três instâncias com o mesmo banco e as mesmas filas:

```sh
docker compose up -d --build --wait --scale app=3
```

As portas disponíveis são `3000`, `3001` e `3002`; o Compose atribui uma a cada container. Consulte `docker compose ps`. Cada instância executa HTTP, consumidor, publisher e worker de referências, sem coordenação em memória entre processos.

## Desenvolvimento local

```sh
bun install --frozen-lockfile
cp .env.example .env
bun run infra:up
bun run migration:run
bun run dev
```

Ao usar a aplicação no host, interrompa antes as instâncias do serviço `app` que ocupam a porta 3000: `docker compose stop app`. O Bun carrega `.env` automaticamente. Os defaults de desenvolvimento estão em `.env.example`; o Compose configura os endereços internos separadamente.

## Verificação

Tudo pode ser executado dentro do Docker:

```sh
docker compose up -d --wait postgres localstack
docker compose --profile test run --build --rm tests
```

Ou com Bun no host:

```sh
bun run typecheck
bun run test:unit
bun run test:integration
bun run build
bun run format:check
```

`bun run test:all` reúne typecheck, unidade com cobertura e integração. A suíte de integração usa **PostgreSQL e SQS reais**: cria um banco temporário e filas com namespace único, executa migrations `up → down → up` e remove esses recursos ao terminar. O usuário do banco de testes precisa de `CREATEDB`; pode ser configurado por `TEST_DATABASE_ADMIN_URL`. Não aponte esses testes para produção.

Os testes incluem 50 apostas simultâneas, disputa de saldo, wallets independentes, três processos Bun/Nest, dois publishers, morte após commit e antes do ack, redelivery, retries, DLQ, referências fora de ordem, reinício, constraints e rollback atômico. Após cada teste de integração, todas as wallets são comparadas ao ledger e à versão esperada. A matriz completa está em [REQUIREMENTS.md](docs/REQUIREMENTS.md). A auditoria executada, com 35 testes unitários e 23 de integração, logs e resultados por requisito, está em [VERIFICATION.md](docs/VERIFICATION.md).

Com uma instância ativa e workers habilitados, execute uma demonstração HTTP + SQS:

```sh
bun run demo
```

Ela cria uma wallet, envia a mesma aposta 50 vezes em paralelo, publica um reembolso por SQS e verifica a reconciliação final. Usa dados novos em cada execução. `DEMO_BASE_URL` permite selecionar outra instância.

Para verificar health e métricas nas três réplicas, incluindo falhas reais das dependências:

```sh
bun scripts/verify-runtime.ts
```

Essa sonda é exclusiva do Compose local: pausa temporariamente o LocalStack, interrompe o PostgreSQL e restaura cada serviço em `finally`. Execute sem outras tarefas usando essa infraestrutura.

## Contrato HTTP

Valores monetários são objetos `{ "amount": "25.00", "currency": "BRL" }`. `amount` deve ser uma **string não negativa com exatamente duas casas decimais**. Números JSON, expoentes e arredondamento implícito são rejeitados. São suportadas `BRL`, `USD` e `EUR`; não há conversão cambial.

| Método | Rota                                                                  | Finalidade                                           |
| ------ | --------------------------------------------------------------------- | ---------------------------------------------------- |
| POST   | `/wallets`                                                            | Criar wallet e registrar abertura positiva no ledger |
| GET    | `/wallets/:walletId`                                                  | Consultar saldo e versão                             |
| GET    | `/wallets/:walletId/ledger?limit=50&cursor=...`                       | Paginar ledger em ordem crescente, cursor opaco      |
| POST   | `/wagering/transactions`                                              | Submeter operação; exige `Idempotency-Key`           |
| GET    | `/wagering/transactions/:transactionId`                               | Consultar transação pelo ID interno                  |
| GET    | `/providers/:providerId/wagering/transactions/:externalTransactionId` | Consultar pela identidade do provedor                |
| POST   | `/wallets/:walletId/reconciliation`                                   | Comparar saldo persistido com soma do ledger         |
| GET    | `/health/live`                                                        | Liveness, sem autenticação                           |
| GET    | `/health/ready`                                                       | Readiness de PostgreSQL e das filas SQS              |
| GET    | `/metrics`                                                            | Métricas em formato Prometheus                       |

Criar wallet:

```sh
curl --fail-with-body -X POST http://localhost:3000/wallets \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Use o `id` retornado como `walletId`:

```sh
curl --fail-with-body -X POST http://localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -H 'X-Correlation-Id: example-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"SUBSTITUA_PELO_ID_RETORNADO","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

Resposta de sucesso:

```json
{
  "transactionId": "0192f298-345e-7e38-af88-e43f851a819d",
  "status": "PROCESSED",
  "balance": { "amount": "75.00", "currency": "BRL" },
  "idempotentReplay": false
}
```

Repetir a mesma operação com a mesma chave retorna o resultado persistido, inclusive o saldo original, com `idempotentReplay: true`. A chave é globalmente única; a identidade `(providerId, externalTransactionId)` também é única. Alterar o payload ou trocar a chave dessa identidade gera conflito.

| HTTP | Significado                                               |
| ---- | --------------------------------------------------------- |
| 201  | Wallet criada                                             |
| 200  | Consulta, operação processada ou replay processado        |
| 202  | Referência pendente; processamento será retomado          |
| 400  | Contrato inválido, dinheiro inválido ou cursor inválido   |
| 404  | Wallet ou transação inexistente                           |
| 409  | Wallet duplicada ou conflito de idempotência              |
| 422  | Rejeição de negócio persistida, com `failureCode`         |
| 503  | Infraestrutura indisponível ou transação técnica `FAILED` |

Respostas de erro de contrato/infraestrutura usam `{ "code": "...", "correlationId": "..." }`. Resultados de negócio usam o envelope da transação, com `status` e `failureCode`. Uma falha `FAILED` já persistida é terminal: consulte sua identidade antes de iniciar recuperação operacional.

`REFUND` e `ROLLBACK` exigem `referenceExternalTransactionId`. `LOSS` exige `0.00` e não movimenta saldo. As interpretações e códigos de rejeição estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Filas e eventos

| Fila FIFO                     | Uso                                         |
| ----------------------------- | ------------------------------------------- |
| `wager-transactions.fifo`     | Entrada de operações                        |
| `wager-transactions-dlq.fifo` | Mensagens inválidas ou tentativas esgotadas |
| `wager-events.fifo`           | Eventos publicados pela outbox              |

Envie com `MessageGroupId=walletId` e um `MessageDeduplicationId`. A correção financeira continua garantida no PostgreSQL mesmo se o produtor escolher grupos diferentes. O envelope de entrada é:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-07-29T15:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

O `messageId` é estável nas reentregas. A inbox usa `(consumerName, messageId)`; o hash da inbox inclui a chave do comando, enquanto o hash financeiro exclui metadados de transporte e a chave de idempotência.

Eventos versionados: `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference`, `WalletBalanceChanged` e `WagerTransactionFailed`. A entrega é **at-least-once**; consumidores de eventos precisam persistir deduplicação por `eventId`, mesmo com FIFO.

## Operação local

```sh
docker compose logs -f app
docker compose restart app
docker compose stop app
docker compose run --rm migrate bun run migration:revert
docker compose down
```

`migration:revert` remove o schema e seus dados: use apenas para testar reversibilidade em ambiente descartável, com a aplicação parada. `docker compose down` preserva volumes; `docker compose down --volumes` apaga os dados locais.

As principais configurações são `SQS_MAX_ATTEMPTS`, `REFERENCE_MAX_ATTEMPTS`, `REFERENCE_BACKOFF_MS`, `DB_LOCK_TIMEOUT_MS`, `WORKER_INTERVAL_MS` e `SHUTDOWN_TIMEOUT_MS`. `SQS_QUEUE_PREFIX` permite isolar filas já provisionadas. O timeout padrão de encerramento é 20 s, com grace period de 30 s no Compose. O redrive da fila é provisionado com cinco recebimentos; ao alterar o limite da aplicação, ajuste também a política SQS conforme explicado na arquitetura.

## Organização e decisões

```text
src/domain/             Money, Wallet, transações, ledger, inbox, outbox e eventos
src/application/        casos de uso, consultas, portas e workers
src/infrastructure/     TypeORM, migrations, SQS, configuração e telemetria
src/http/               controllers e tradução de erros NestJS
tests/unit/             invariantes do domínio e contratos
tests/integration/      PostgreSQL/SQS reais, processos e falhas
tests/fixtures/         processos usados nos testes de concorrência/crash
scripts/                migrations e demonstração executável
```

Consulte [ARCHITECTURE.md](ARCHITECTURE.md) para transações, locks, constraints, hash canônico, estados, retries e limitações. A [matriz de requisitos](docs/REQUIREMENTS.md) relaciona o enunciado ao código e aos testes.

Autenticação foi omitida conforme permitido pelo desafio. Existe uma porta explícita para integrar um IdP OIDC externo. Este ambiente local não oferece isolamento autenticado entre provedores; os controles necessários para produção estão documentados na arquitetura.
