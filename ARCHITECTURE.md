# Pushkin — архитектура кода

Этот документ задаёт правила устройства Go-кода Pushkin. Он дополняет
[`SPECIFICATION.md`](SPECIFICATION.md): спецификация описывает поведение
системы, а этот документ — где живёт код и от каких слоёв он может зависеть.

## Подход

Pushkin — один bounded context: платформа управления кампаниями и доставки
push-уведомлений. Внутри него используются:

- **DDD** для бизнес-правил кампании, mobile applications, push installations,
  tenant, channel и provider;
- **CQRS** для control plane API: команды меняют состояние, queries возвращают
  read-модели;
- **Hexagonal Architecture (Ports and Adapters)** на границах с PostgreSQL,
  Kafka, Redis, FCM и HTTP;
- **application services** для фоновых задач и Kafka pipeline.

Доставка — часть доменной области Pushkin. Но единица доставки — не persistence
aggregate: её нельзя сохранять как миллионы строк или проводить через repository
на каждый push. Доменная `DeliveryWork` — богатая immutable-модель, а Kafka
`DeliveryWorkV1`, retry record и offset — её транспортное представление и
технические детали pipeline.

## Слои и направление зависимостей

```text
interfaces                       HTTP, внешняя и внутренняя Kafka
        ↓
application                      commands, queries, background services
        ↓
domain                           Campaign, MobileApplication/PushInstallation, Tenant/Channel/Provider model
        ↑
application ports                interfaces внешних зависимостей
        ↑
infrastructure                   PostgreSQL, Kafka, Redis, FCM
```

Правила:

1. `domain` не импортирует `application`, адаптеры, Kafka-клиент, SQL, Redis,
   HTTP или конкретные конфигурации.
2. `application` импортирует `domain` и собственные ports, но не реализации
   адаптеров.
3. Адаптеры реализуют ports и могут импортировать `application`/`domain`, но
   не содержат бизнес-решений.
4. `bootstrap` — единственное место, где допустимо связывать конкретные
   реализации с ports.
5. Общая техническая утилита не должна импортировать domain. Если ей нужен
   бизнес-смысл, она принадлежит application или domain.

## Структура каталогов

```text
api/
  http/
    v1/openapi.yaml               внешний HTTP-контракт
  kafka/
    v1/                           внешние Kafka commands/events

cmd/
  server/                         production binary v1
  fake-fcm/                       test-only HTTP provider для Compose и benchmark

internal/
  domain/                        единый package: Campaign, SourceBatch,
                                 MobileApplication, PushInstallation, Channel,
                                 Provider, payload, priority и инварианты

  application/
    commands/                    write-side use cases control plane
    queries/                     read-side use cases и read models
    services/                    background и Kafka pipeline use cases
    ports/                       repository и external-service interfaces

  contracts/
    kafka/                        внутренние versioned pipeline contracts

  interfaces/
    http/                         handlers, DTO, auth middleware, router
    kafka/                        consumer внешних user events

  infrastructure/
    postgres/                     repositories, migrations, SQL queries/mappers
    kafka/                        producers, transactional consumer support
    redis/                        distributed rate limiter
    providers/
      fcm/                        Firebase adapter (v1)

  bootstrap/                     composition root и wiring
```

В v1 `cmd/server` запускает HTTP server и все worker loops через один
`bootstrap.Module`. Это упрощает разработку и deployment, не смешивая слои
кода: HTTP и Kafka остаются разными входными interfaces.

### Будущее разделение deployment roles

При необходимости независимого масштабирования один binary может запускать
выбранные роли через конфигурацию, например `PUSHKIN_ROLES=delivery,retry`.
Один Docker image тогда разворачивается несколькими Kubernetes Deployment.

Если роли станут существенно различаться по lifecycle и зависимостям, следующий
шаг — несколько `cmd` (`api`, `scheduler`, `worker`, `aggregator`) и отдельный
bootstrap composition root для каждого. Domain, application services и contracts
при этом не меняются. Каждый bootstrap создаёт только нужный dependency graph;
например, scheduler не должен подключать FCM или HTTP server.

## Доменная модель

### Campaign

`Campaign` — основной агрегат control plane. Только его методы меняют
состояние кампании. Они проверяют:

- допустимость переходов `DRAFT → SCHEDULED → STARTING → STARTED → COMPLETED`;
- невозможность добавлять source batches после `start`;
- корректность `scheduled_at`, payload, priority и channel;
- fencing через `run_id` при запуске.

Методы агрегата не публикуют Kafka-сообщения и не обращаются к репозиториям.
Они возвращают обычные значения, например новый `run_id` или ошибку перехода.

`SourceBatch` логически принадлежит Campaign, но хранится и загружается отдельно:
в нём могут быть до 5 000 `user_id`. Для операций импорта не нужно гидратировать
весь Campaign вместе со всеми batch-ами.

### MobileApplication и PushInstallation

`MobileApplication` — platform-specific приложение tenant-а с `platform`,
`package_name` и одним FCM Provider. Несколько applications могут использовать
один Provider. `Channel` типа `mobile_push` принадлежит одному Provider и
выбирает одно или несколько его applications. Это проверяет
`ChannelMobileApplicationService` при изменении связи.

`PushInstallation` принадлежит пользователю и MobileApplication; стабильный
`installation_id` позволяет обновлять token при refresh. Ответ провайдера о
`unregistered`/`invalid token` переводит installation в неактивное состояние
через application use case. Token не попадает в read-модели или логи.

### Tenant, Channel и Provider

`Channel` — настроенный tenant-ом логический способ отправки с `ChannelType`
(`mobile_push`, `sms`, `email`). `Provider` — конкретная
настроенная интеграция с `ProviderType` (`fcm`, `twilio`, `ses` и т. д.),
зашифрованными credentials, параметрами и QPS/burst лимитами. В v1 Channel типа `mobile_push`
принадлежит одному FCM Provider и выбирает одно или несколько его
MobileApplication; fallback/split traffic отсутствуют.

Они не являются горячими агрегатами delivery path. `users` также не
доменная сущность, а техническая проекция внешнего user service. FCM может
обслуживать Android и iOS applications через одну provider configuration.

### Delivery

`DeliveryWork` принадлежит домену Pushkin и хранит routing fields и число уже
выполненных retry-волн; его метод увеличивает счётчик не более трёх раз.
`CampaignProgress` — внутренний компонент `Campaign` без собственной
идентичности; он выражает условие завершения кампании.

Классификация ответа конкретного провайдера, выбор retry bucket и `due_at`
принадлежат application/infrastructure коду. Домен не знает о Kafka
topic/partition/offset, Redis или HTTP-кодах FCM.

## Команды и queries

Команды — публичные write-side use cases, вызываемые HTTP или Kafka interfaces.
Примеры:

```text
CreateCampaign
AddRecipientBatch
StartCampaign
RegisterPushInstallation
ApplyUserEvent
```

Команда открывает короткую PostgreSQL-транзакцию, применяет доменные правила,
сохраняет состояние и возвращает ID либо скалярный результат. HTTP handler не
содержит бизнес-логики: он валидирует DTO, извлекает API key/actor, вызывает
команду и переводит application error в HTTP-ответ.

Queries не возвращают domain aggregates. Они читают текущую статистику из
`campaigns` и другие специализированные read-модели, возвращая DTO/read model
для API.

## Application services и background jobs

Каждая фоновая роль вызывает application service; consumer, cron loop и Kafka
client остаются адаптерами. Основные services:

| Service | Вход | Ответственность |
| --- | --- | --- |
| `CampaignSchedulerService` | timer / DB polling | Находит due `SCHEDULED`, свежие `STARTING` без attempt и застрявшие `STARTING`; создаёт fencing `run_id`, фиксирует `run_attempted_at`, публикует `CampaignRunRequested`. |
| `BatchedCampaignRunCoordinatorService` | `CampaignRunRequested` | Проверяет `run_id`, ставит `STARTED`, транзакционно создаёт source-batch fanout work. |
| `BatchedSourceBatchFanoutService` | `SourceBatchFanout` | Разрешает user ID в push installations и создаёт individual delivery work. |
| `InlineCampaignRunCoordinatorService` | `CampaignRunRequested` | Пачкой запускает inline-кампании и создаёт по одному inline fanout work. |
| `InlineCampaignFanoutService` | `InlineCampaignFanout` | Пачкой раскрывает bounded inline recipients в individual delivery work. |
| `DeliveryService` | delivery work/retry | Берёт rate-limit permits, вызывает FCM, создаёт retry или progress delta. |
| `RetryDeliveryService` | time-bucket retry topic | Pause-ит partition до `due_at`, затем выполняет provider attempt прямо из retry topic и создаёт terminal result либо следующую retry-волну. |
| `CampaignProgressAggregatorService` | `pushkin.campaign.progress` | Строит compacted `pushkin.campaign.stats`. |
| `CampaignStatsProjectionService` | `pushkin.campaign.stats` | Применяет current progress к Campaign в PostgreSQL и завершает её при достижении terminal state. |
| `ChannelProvisioningService` | PostgreSQL polling | Находит активные Channel, для которых нужно создать или остановить local workers. |
| `UserCommands` | external user events через Kafka interface | Идемпотентно обновляет локальную доменную копию User. |

Service получает зависимости только как ports. Его unit-тест не требует
PostgreSQL, Kafka или FCM: используются fake implementations ports.

## Контракты и Kafka pipeline

Корневой `api/` содержит только внешний API Pushkin — то, что обязуются
поддерживать внешние интеграторы. HTTP source of truth — `api/http/v1/openapi.yaml`;
generated HTTP types и DTO принадлежат `internal/interfaces/http`.

`api/kafka/v1` содержит внешние Kafka-контракты user service, если Pushkin
принимает их как часть объявленного контракта. Управление кампаниями и импорт
аудитории в v1 выполняются только через HTTP API.

Внутренние сообщения pipeline хранятся в `internal/contracts/kafka`. Они нужны
внутренним producer/consumer ролям и могут меняться вместе с реализацией. Их
versioning сохраняется ради безопасного rolling deployment, но это не публичное
обещание совместимости.

Kafka-сообщения — versioned технические контракты. Они не являются domain events
и не сериализуют domain aggregates напрямую. Delivery-контракт может переносить
данные domain `DeliveryWork`, но добавляет transport-поля: schema version,
partition key и время retry bucket-а. Контракт содержит только нужные pipeline
поля, например:

```text
CampaignRunRequestedV1
SourceBatchFanoutV1
InlineCampaignFanoutV1
DeliveryWorkV1
RetryWorkV1
SourceBatchFanoutCompletedV1
InlineCampaignFanoutCompletedV1
CampaignProgressDeltaV1
CampaignStatsSnapshotV1
```

Изменение несовместимого сообщения создаёт новую версию контракта, а не
незаметно меняет существующую схему. Каждый consumer валидирует версию,
обязательные поля, размер и допустимые enum-значения до бизнес-обработки.

`interfaces/kafka` — primary adapter, эквивалентный HTTP interface: он читает и
валидирует record, затем вызывает application command/service. Он не содержит
доменных решений. Kafka producer, connection management и transaction support
живут в `infrastructure/kafka`.

### Граница Kafka-транзакции

Один Kafka `DeliveryWorkV1` представляет одну domain `DeliveryWork`. Worker
может собрать до 100 таких records, выполнить provider calls параллельно и
зафиксировать их outcomes одной Kafka transaction:

```text
consume up to 100 records
  → parallel provider calls вне Kafka transaction
  → begin Kafka transaction
  → produce individual retry и CampaignProgressDelta для terminal outcomes
  → send только непрерывные input offsets по каждой partition
  → commit
```

Внешний вызов не бывает атомарным с Kafka. Поэтому Pushkin гарантирует
at-least-once до FCM, а не exactly-once фактическую доставку.

Несколько records одной partition могут быть отправлены параллельно внутри одной
группы, но их offsets фиксируются только общим непрерывным префиксом после
получения всех outcome. Один pod ограничивает общее число in-flight provider
calls через semaphore; глобальные tenant/provider quotas берутся атомарно из
Redis.

При обычном ожидании rate-limit permit service pause-ит назначенные partitions,
сохраняя heartbeat/poll loop consumer group. Проверка provider quota всегда
происходит до захвата pod semaphore, чтобы заблокированный Provider не занял всю
ёмкость. `429`/`Retry-After` переводят work в delay/retry topic и коммитят
исходный offset транзакционно.

Delivery topics разделяются по `channel_id`. Это изолирует backlog и приоритеты
логического Channel. Provider выбирается из snapshot конфигурации Channel; в v1
он единственный. Изоляция topic не заменяет Redis limiter: несколько pod-ов одной
consumer group всё ещё делят quota выбранного Provider.

### Stateful aggregator

`CampaignProgressAggregatorService` владеет состоянием кампаний своей
`pushkin.campaign.progress` partition. Перед обработкой он восстанавливает map из
соответствующей partition compacted topic `pushkin.campaign.stats`, затем в Kafka
transaction публикует новый абсолютный snapshot и коммитит progress offsets.
Он не делает `UPDATE campaigns SET sent_count = sent_count + ...` на каждый
delivery item.

## Ports

Порт создаётся, только если он отделяет application от реального внешнего
ресурса или заметной политики. Ожидаемые ports:

```text
CampaignRepository, SourceBatchRepository, PushInstallationRepository
TransactionManager
KafkaConsumer, KafkaProducer, KafkaTransactionalConsumer, CompactedTopicLoader
ChannelTopicProvisioner, DeliveryRateLimiter, PushCallSemaphore, PushSender
CredentialsCipher, tenant API-key validator/hasher
```

Не следует вводить интерфейс для каждого private helper или каждой структуры.
Небольшая чистая логика остаётся обычной функцией/типом в нужном пакете.

## Ошибки и идемпотентность

- Domain/application errors имеют typed kind: validation, conflict, not found,
  forbidden, transient dependency, permanent dependency.
- Adapter переводит их в HTTP/Kafka решение; domain не знает HTTP status codes.
- Идемпотентность внешних запросов появится после v1 только для операций, где
  повтор создаёт новый эффект; её durable state не хранится в памяти процесса.
- Kafka handler не commit'ит offset, пока application service не завершил
  требуемую Kafka-транзакцию.
- Каждый внешний вызов получает `context.Context` с deadline. Завершение pod'а
  прекращает intake, ждёт ограниченное время активные операции и только потом
  завершает consumer.

## Конфигурация и секреты

Конфигурация читается один раз в `bootstrap`, валидируется при старте и
передаётся через typed config. Нельзя читать environment variables из domain,
application services или адаптеров по месту использования.

API keys, FCM credentials и Redis/PostgreSQL/Kafka DSN не логируются.
Секреты передаются в bootstrap через environment variables. Production deployment
должен поставлять их из Kubernetes Secret или внешнего secret manager и поддерживать
ротацию без публикации значений в метрики.

## Наблюдаемость

Pushkin экспортирует opt-in metrics через OTLP; без endpoint используется
no-op provider. Локальный Compose stack включает OTel Collector, Grafana LGTM
и exporters PostgreSQL, Redis и Kafka. Tracing, production dashboards, alert
rules и SLO остаются отдельной последующей работой. Internal Kafka-контракты
не содержат trace/correlation-полей.

## Тесты

- `domain`: table-driven unit tests всех переходов состояния и инвариантов.
- `application`: unit tests services с fakes ports, включая fencing и retry
  decisions.
- `infrastructure/postgres`: integration tests миграций, SQL и транзакций.
- `infrastructure/kafka`: integration tests Kafka-транзакций и topology.
- Локальный Compose и benchmark-сценарии проверяют полный delivery pipeline с
  Fake FCM; полноформатные E2E tests остаются отдельной работой.

## Осознанно не используем в v1

- Domain events, in-memory event dispatcher и event sourcing.
- Transactional outbox в delivery hot path.
- Repository/aggregate на каждую delivery attempt.
- Общие mutable singleton maps для состояния кампаний: durable state агрегатора
  находится в compacted Kafka topic, а локальная map принадлежит partition owner.

Если появится независимый локальный reaction на переход агрегата после commit,
domain event можно добавить точечно. Если появится несколько bounded contexts
или внешние подписчики на события Pushkin, integration events и outbox следует
спроектировать отдельным ADR, а не внедрять неявно в существующий pipeline.
