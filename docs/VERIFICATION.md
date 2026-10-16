# Evidências de conformidade — 16/10/2026

## Referência e conclusão

A auditoria releu o [enunciado oficial](https://github.com/junglegaming/backend-challenge/blob/c7143e6d041585d6913f56eb7415105e074f83dc/README.md), commit `c7143e6d041585d6913f56eb7415105e074f83dc`, e confrontou cada obrigação com código, testes e execução. A [matriz completa](REQUIREMENTS.md) identifica a implementação de cada requisito.

**Resultado da verificação: todos os requisitos obrigatórios mapeados têm implementação e evidência aprovada.** Foram executados 35 testes unitários e 23 testes de integração, tanto no host quanto no container. Os testes financeiros usam PostgreSQL e LocalStack reais. Isso demonstra os cenários descritos; não constitui prova matemática para toda combinação de falhas nem certificação de produção.

## Execuções preservadas

Todos os links abaixo apontam para saídas reais. Os arquivos JSON correspondentes registram comando, horário com fuso, duração, exit code e SHA-256 da saída. O [README original do desafio](evidence/2026-10-16/upstream-README.txt) também foi preservado sem edição. O [manifesto](evidence/2026-10-16/manifest.json) identifica os arquivos de código/configuração verificados, seus hashes e o ambiente; permite distinguir este resultado de alterações futuras.

| Verificação                   | Resultado                                                                                    | Saída original                                           |
| ----------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| TypeScript estrito            | Exit 0                                                                                       | [typecheck.txt](evidence/2026-10-16/typecheck.txt)       |
| Build                         | Exit 0                                                                                       | [build.txt](evidence/2026-10-16/build.txt)               |
| Formatação                    | Exit 0                                                                                       | [format.txt](evidence/2026-10-16/format.txt)             |
| Unidade no host               | 35 pass, 0 fail; 132 assertions                                                              | [unit.txt](evidence/2026-10-16/unit.txt)                 |
| Integração no host            | 23 pass, 0 fail; 206 assertions                                                              | [integration.txt](evidence/2026-10-16/integration.txt)   |
| Suíte no Docker               | Typecheck + 35 unitários + 23 de integração, sem falhas                                      | [docker-tests.txt](evidence/2026-10-16/docker-tests.txt) |
| Compose com três réplicas     | PostgreSQL, LocalStack e três apps saudáveis                                                 | [compose.txt](evidence/2026-10-16/compose.txt)           |
| Demonstração HTTP + SQS       | 50 requests, 1 aplicação, 49 replays, refund processado                                      | [demo.txt](evidence/2026-10-16/demo.txt)                 |
| Falhas reais das dependências | SQS e PostgreSQL indisponíveis: ready 503 e live 200 nas três réplicas; recuperação para 200 | [runtime.txt](evidence/2026-10-16/runtime.txt)           |
| Estado e versões do ambiente  | Containers e versões após a recuperação                                                      | [environment.txt](evidence/2026-10-16/environment.txt)   |

## O que cada grupo comprova

As descrições de teste abaixo aparecem literalmente nos logs. Os testes estão em [domain.test.ts](../tests/unit/domain.test.ts), [telemetry.test.ts](../tests/unit/telemetry.test.ts) e [system.test.ts](../tests/integration/system.test.ts).

| Obrigação do enunciado                            | Evidência executada e resultado observado                                                                                                                                                                                                    |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stack e ORM aceitos; TS estrito; Bun              | Build/typecheck no host e Docker; serviço NestJS com TypeORM, PostgreSQL e SQS/LocalStack reais. Configurações e versões no manifesto.                                                                                                       |
| Migrations versionadas e reversíveis              | Setup de integração executa `up → down → up`; teste `reversible migrations, opening audit, uniqueness, immutability and constraints`.                                                                                                        |
| Money exato, escala 2 e imutável                  | Unidade testa aritmética centesimal e valores acima de `Number.MAX_SAFE_INTEGER`, sem arredondamento; 15 entradas inválidas, dinheiro numérico, overflow e conflito de moedas rejeitados. Persistência de `9007199254740993.01` é exata.     |
| Modelos encapsulados, factories, rehydrate        | Inspeção das classes de domínio; unidade testa factories, reidratação, transições e imutabilidade. Domínio independente de Nest/ORM.                                                                                                         |
| Wallet única, saldo não negativo, versão e ledger | SQL real rejeita wallet duplicada, saldo negativo, saldo/versão alterados sem ledger e lançamento duplicado; testes de domínio verificam incremento somente com mudança de saldo.                                                            |
| Ledger append-only                                | UPDATE, DELETE e TRUNCATE rejeitados pelo PostgreSQL; factory rejeita aritmética inconsistente.                                                                                                                                              |
| Abertura auditável e interna                      | Abertura positiva persiste OPENING + CREDIT com versão 1; unidade e HTTP rejeitam OPENING público; a inspeção do contrato confirma que SQS reutiliza o mesmo schema restrito.                                                                |
| BET, WIN e LOSS                                   | Unidade cobre cada operação; integração testa crédito, débito e LOSS sem alteração financeira nem evento de saldo.                                                                                                                           |
| REFUND/ROLLBACK integrais, únicos e compatíveis   | Unidade e integração verificam referência, identidade, moeda, rodada, valor integral, tipo permitido e duplicação. Reversão que deixaria saldo negativo recebe código específico e não altera o ledger.                                      |
| Estados, falhas e replay original                 | Testes verificam estados terminais imutáveis, retomada pendente e códigos estáveis; replay retorna saldo original mesmo após outra operação.                                                                                                 |
| Idempotência persistente e hash canônico          | Hash recursivo com chaves ordenadas; payload divergente conflita; identidade externa não aceita outra key; redelivery e reinício preservam deduplicação.                                                                                     |
| Header como fonte da key e contratos estritos     | HTTP testa header obrigatório, rejeita body indevido, dinheiro numérico e payload inválido; unidade testa envelope SQS.                                                                                                                      |
| Atomicidade wallet/transação/ledger/inbox/outbox  | Trigger de teste provoca falha na inbox depois das escritas financeiras: rollback integral das cinco estruturas.                                                                                                                             |
| 50 apostas iguais paralelas                       | `50 simultaneous identical bets debit once, without FIFO`: um transactionId, um débito, saldo 75.00; demo pública confirma 1 aplicação + 49 replays.                                                                                         |
| Duas apostas de 80.00 sobre 100.00                | `two simultaneous 80.00 bets from 100.00 give one debit, one business rejection and 20.00`: PROCESSED + REJECTED/INSUFFICIENT_FUNDS, um débito, saldo 20.00 inclusive após retries.                                                          |
| Wallets independentes                             | Uma wallet fica bloqueada por transação SQL real; outra conclui. Depois, 12 wallets processadas em paralelo preservam saldo/ledger.                                                                                                          |
| Três processos, concorrência e reinício           | Três processos Bun/Nest recebem 50 requests distribuídos; apenas uma aplicação; encerramento e novo processo preservam replay e reconciliação. Compose adicional sobe três containers reais.                                                 |
| Referência fora de ordem                          | REFUND antes de BET persiste pendência; retry aplica uma única vez após referência. Esgotamento do limite rejeita com REFERENCE_NOT_FOUND e evento na outbox.                                                                                |
| API e códigos distintos                           | Teste HTTP cobre todas as rotas exigidas, criação, consultas por ID interno/externo, reconciliação e respostas 200/201/202/400/404/409/422/503.                                                                                              |
| Cursor estável e opaco                            | Cursor limitado à wallet, limite validado e fronteira superior preservada diante de novas inserções; páginas não repetem entradas.                                                                                                           |
| Reconciliação sem reparo silencioso               | Teste adultera saldo para 99.00, mantendo ledger 100.00: resposta consistent=false/difference=-1.00, métrica incrementada e log de divergência. Saldo permanece 99.00 até restauração explícita do fixture.                                  |
| Filas/envelope e mesmo caso de uso                | Integração envia envelope real ao SQS; consumidor e HTTP usam WageringService; nomes e inicialização inspecionados.                                                                                                                          |
| Inbox, ack após commit e redelivery               | Envelope repetido gera uma inbox e um débito; SIGKILL depois do commit e antes do ack permite redelivery sem novo efeito.                                                                                                                    |
| Retry, backoff, limite, FAILED e DLQ              | Falha de lock real retorna mensagem com visibilidade/backoff; retry conclui. Entrada permanente e tentativas esgotadas enviam corpo original à DLQ real; FAILED fica auditável quando o banco está disponível.                               |
| SIGTERM e mensagem em andamento                   | Dois testes lançam src/main.ts, bloqueiam sua transação no PostgreSQL e enviam SIGTERM. Um conclui no prazo; outro devolve visibilidade ao esgotar o prazo. Ambos encerram com exit 0 e mantêm exatamente um débito após redelivery.         |
| Outbox pós-commit e recuperação                   | Linha ainda não confirmada não é publicada; erro de envio persiste backoff; novo publisher drena pendências confirmadas. Dois publishers em processos distintos concorrem sem perder eventos.                                                |
| Duplicação de evento e envelopes                  | eventId persistente e deduplication ID estável; unidade valida classe abstrata/subclasses dos quatro eventos, versão e JSON com MoneyProps. Consumidores externos devem deduplicar por eventId, conforme contrato at-least-once documentado. |
| Observabilidade                                   | Unidade verifica os cinco identificadores de rastreio e exclusão de campos sensíveis; sete famílias de métricas testadas com valores/labels e expostas nas três réplicas; divergência também tem contador e log.                             |
| Liveness/readiness e dependências reais           | Sonda pausa LocalStack e depois para PostgreSQL: live=200 e ready=503 nas três réplicas; após restaurar cada dependência, ready=200 sem reiniciar os apps.                                                                                   |
| Invariante final                                  | afterEach compara todas as wallets do banco de integração à soma assinada do ledger e à versão esperada; demo termina storedBalance=calculatedBalance=100.00, difference=0.00, consistent=true, 3 entradas.                                  |
| README e arquitetura                              | Setup reproduzido, comandos de teste/build/Compose executados; escolhas, mapeamento monetário, locking, auth, taxonomia de falhas e limitações documentados.                                                                                 |

A corrupção do fixture usa privilégios administrativos somente no banco temporário, desativando triggers dentro de uma transação; o cenário restaura o saldo em finally antes da checagem global. As garantias normais do schema continuam ativas nos demais testes.

## Falha encontrada e corrigida nesta auditoria

A primeira sonda de indisponibilidade falhou: com o LocalStack pausado, o readiness não respondeu dentro de 20 segundos. O cliente HTTP da versão instalada do SDK interpretava requestTimeout como aviso, pois throwOnRequestTimeout não estava habilitado. A [saída anterior à correção](evidence/2026-10-16/runtime-before-fix.txt) foi preservada com exit 1.

A correção em [sqs.ts](../src/infrastructure/sqs.ts) habilita throwOnRequestTimeout=true. Foi adicionado um teste de regressão com conexão HTTP real que aceita o request e nunca responde: readiness deve retornar 503 dentro de 15 segundos e liveness permanece 200. A sonda completa foi repetida sobre as três imagens reconstruídas, agora com sucesso, incluindo a indisponibilidade de PostgreSQL e a recuperação de ambas as dependências.

Também foram acrescentadas evidências que antes estavam fracas: logs seguros, valores das métricas, divergência deliberada de reconciliação e SIGTERM no processo principal com mensagem em andamento. São dois testes unitários e quatro testes de integração adicionais.

## Reprodução e limites

```sh
bun install --frozen-lockfile
docker compose up -d --wait postgres localstack
bun run format:check
bun run test:all
bun run build
docker compose --profile test run --build --rm tests
docker compose up -d --build --wait --scale app=3
bun run demo
bun scripts/verify-runtime.ts
```

A última sonda interrompe temporariamente dependências do Compose local e as restaura em finally. Não execute com outras tarefas usando essa infraestrutura. Os testes de integração usam banco/filas temporários e removem somente seus próprios recursos; a demonstração cria dados novos no banco local de desenvolvimento.

A cobertura unitária foi **86,93% das linhas dos arquivos carregados nessa execução**, com Money e contratos em 100%. Esse número não mede cobertura de todo o repositório nem de todos os caminhos distribuídos. A suíte é uma verificação de correção, não um benchmark de throughput ou um teste exaustivo de caos.

Autenticação externa foi deliberadamente omitida pela opção aceita na seção 2: há porta de extensão e desenho OIDC em ARCHITECTURE.md. Double-entry, OpenTelemetry, dashboard e teste de carga são opcionais e não compõem a conclusão sobre requisitos obrigatórios.
