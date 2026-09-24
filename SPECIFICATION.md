# Pushkin — спецификация первой версии

## 1. Назначение

Pushkin — self-hosted платформа для отправки мобильных push-уведомлений. Она
устанавливается в Kubernetes-кластере компании и принимает запросы от её
внутренних систем. Первая версия отправляет мобильные уведомления через FCM для
Android и iOS. Архитектура должна позволять позднее добавить прямой APNs, SMS,
e-mail, Web Push и другие каналы как новые delivery adapters.

Типичные сценарии: критичные уведомления об абонентском счёте или блокировке,
продуктовые уведомления о статусах и массовые кампании — например, для
участников розыгрыша.

## 2. Границы первой версии

Включено:

- Мультиарендность: `tenant` (компания/контур), его `channel` и `provider`.
  В v1 `channel_type = mobile_push`, `provider_type = fcm`.
- Регистрация и жизненный цикл push installations и токенов.
- Синхронизация минимальной проекции пользователей из Kafka.
- Транзакционные отправки по одному или нескольким `user_id`.
- Массовые кампании с импортом готового списка `user_id` через REST.
- Черновик, импорт аудитории, запуск, планирование, ретраи через time buckets
  `1m`/`5m`/`30m` и базовые агрегированные отчёты.
- Приоритеты `critical`, `high`, `normal`.

Не включено:

- Построение сегментов по CRM-данным внутри Pushkin. В первой версии источник
  сам формирует аудиторию и передаёт список единых идентификаторов.
- SMS, e-mail, Web Push и fallback между каналами.
- Гарантированный порядок разных уведомлений пользователю.
- Гарантия фактического показа уведомления на устройстве.

## 3. Цели и нефункциональные требования

- Целевой пиковый поток доставки: **30–40 тыс. push/sec** при горизонтальном
  масштабировании; точная пропускная способность зависит от квот и ответов
  FCM.
- Входной API синхронно валидирует и надёжно сохраняет command: создание
  ресурса отвечает `201 Created`, изменение состояния — `200 OK` или
  `204 No Content`. Ни один ответ не ожидает внешнюю доставку provider-ом.
- Pushkin использует модель **at-least-once** до провайдера. FCM обеспечивает
  доставку best effort;
  редкий повтор после неопределённого сетевого сбоя возможен.
- Критичные уведомления получают приоритет над обычными кампаниями; жёсткая
  гарантия reserved capacity требует отдельной policy и пока не заявляется.
- Система не хранит полный профиль абонента: только единый внешний `user_id`,
  состояние, нужное для уведомлений, и связи с push installations.
- Любая кампания имеет аудит действий.

## 4. Модель данных

`user_id` — единый стабильный строковый идентификатор пользователя, которым
пользуются системы клиента. Внешние resource identifiers (`user_id`,
`channel_key`) передаются строками, чтобы поддержать UUID, subscriber ID, CRM ID
и другие форматы клиента.
Внутренние сущности, созданные Pushkin (`tenant_id`, `channel_id`, `provider_id`,
`campaign_id`, `mobile_application_id`, `push_installation_id`, `delivery_id`,
имеют UUID.
Внешний `channel_key` однозначно сопоставляется внутреннему channel в пределах
tenant. Источником правды о пользователе остаётся внешний user service.

Основные сущности в PostgreSQL:

| Сущность | Назначение |
| --- | --- |
| `tenants` | Изоляция клиентов и глобальные tenant policy/квоты. |
| `providers` | Настроенные tenant-ом provider instances: `provider_type`, зашифрованные credentials, параметры, QPS/burst и status. |
| `channels` | Настроенные tenant-ом логические каналы: `channel_type`, `channel_key`, `provider_id` и status. |
| `mobile_applications` | Platform-specific мобильные приложения: `provider_id` (nullable при отключении), `platform`, `package_name` и status. Несколько приложений могут использовать один Provider. |
| `channel_mobile_applications` | Подмножество mobile applications Provider-а Channel, в которые Channel может отправлять. |
| `users` | Локальная минимальная проекция пользователя. |
| `push_installations` | Установки мобильного приложения: `mobile_application_id → user_id → installation_id → token`; статус и время последнего обновления. |
| `campaigns` | Метаданные, `channel_id`, push payload, приоритет, режим аудитории (`batched` или `inline`), `scheduled_at`, `run_id`, `run_attempted_at`, `started_at`, состояние и текущий `CampaignProgress`. Inline-кампания хранит до 100 неизменяемых `user_id` в самой записи. |
| `campaign_recipient_batches` | Неизменяемые входные чанки аудитории только batched-кампаний и их идентификаторы. |

`ChannelType` и `ProviderType` — enum-ы. `Channel` и `Provider` — конкретные
tenant-scoped записи, а не справочники типов. В v1 `mobile_push` Channel
принадлежит одному FCM Provider и выбирает одно или несколько его
`MobileApplication`; каждое platform-specific приложение также имеет один FCM
Provider. Firebase project, зашифрованные service-account credentials,
`rate_limit_qps` и `rate_limit_burst` принадлежат Provider. Связь
MobileApplication → Provider many-to-one: один Firebase project может
обслуживать несколько приложений. В будущих версиях прямой `provider_id` Channel
будет заменён маршрутами Channel к нескольким Provider для fallback/split traffic.

Инвариант v1: при добавлении MobileApplication в Channel Pushkin проверяет, что
tenant и `provider_id` приложения совпадают с tenant и `provider_id` Channel.
Отключённое приложение с `provider_id = NULL` не участвует в fan-out. Это
удерживает один Channel в пределах одного FCM rate-limit scope; несколько
Provider-ов для одного Channel появятся только вместе с явной routing policy.

Токены нельзя хранить в JSONB-профиле пользователя: они часто меняются, один
пользователь может иметь много installations, а по ним нужны индексы и отдельная
инвалидация. `push_installations` уникальна по
`(tenant_id, mobile_application_id, installation_id)`; token обновляет ту же
installation. Токены и секреты провайдеров шифруются at rest; в логах и
будущей аналитике сохраняются только хеш/маска токена. Invalid installation сначала
деактивируется, а физически удаляется фоновым cleanup после retention.

Списки получателей не разворачиваются заранее в миллионы строк `delivery`.
Для batched-кампании входной batch хранится как неизменяемый чанк и при fan-out
разворачивается в отдельную единицу работы по push installation. Клиентский
batched source batch до 5 000 ID принимается для удобства. Для небольшого one-shot
сценария inline-кампания хранит до 100 неизменяемых `user_id` непосредственно
в `campaigns` и не создаёт source batch. Каждый delivery item имеет стабильный
`delivery_id`; формат ID и предельный размер payload фиксируются контрактом API.

## 5. Синхронизация пользователей и bootstrap

Внешний user service публикует JSON records в topic
`pushkin.user-events.v1`. Каждый public record имеет envelope из
`api/kafka/v1`: `schema_version = 1`, `type = user_event` и `payload`.
Payload — `UserEventV1` со своим `type` (`user_created`, `user_updated` или
`user_deleted`), строковыми `tenant_id`, `user_id` и опциональными
`attributes`. Для created/updated Pushkin выполняет idempotent upsert, для
deleted — idempotent delete.

Логические события представлены как:

- `UserCreated` (`user_created`)
- `UserUpdated` (`user_updated`)
- `UserDeleted` (`user_deleted`)

Ключ партиционирования — canonical JSON array `[tenant_id, user_id]` в UTF-8;
его формирует `api/kafka/v1.PartitionKey`. Все события одного пользователя
обязан публиковать один упорядоченный producer в этот ключ; Pushkin проверяет
его соответствие value и обрабатывает records последовательно в рамках Kafka
partition. Повтор record после падения безопасен: upsert/delete локальной копии
пользователя идемпотентны.

Push installation регистрируется отдельным API через backend клиента; user
service обычно не знает актуальный FCM token. Backend передаёт
`installation_id`, `platform`, `package_name` и token; tenant API key не должен
поставляться в мобильное приложение. Ответы провайдера об
устаревшем/недействительном токене деактивируют installation.

Первичное подключение компании требует backfill, потому что Kafka-топики могут
иметь ограниченный retention, а compacted topic хранит последнее состояние, но
не заменяет согласованную процедуру загрузки:

1. Экспортировать актуальный срез пользователей из source of truth.
2. Импортировать его в Pushkin через защищённый import API или файл в object storage.
3. Зафиксировать watermark — версию либо timestamp среза.
4. Начать или продолжить consumption Kafka с согласованной позиции и сверить
   отсутствующие изменения на границе среза.

## 6. Запуск кампании и fencing

```text
DRAFT → SCHEDULED → STARTING → STARTED → COMPLETED
  │         │           │          │
  └─────────┴───────────┴──────────┴→ FAILED
```

Для **batched** кампании:

1. Отправитель создаёт кампанию с push payload, channel и приоритетом в
   статусе `DRAFT`.
2. Он добавляет произвольное число batch-ей `user_id`.
3. Команда `start` принимает опциональный `scheduled_at`, закрывает импорт
   аудитории и переводит кампанию в `SCHEDULED` (будущее время) или `STARTING`
   (немедленный запуск). После неё новые batch-и отклоняются. В свежем
   `STARTING` поля `run_id` и `run_attempted_at` ещё пусты.

Для **inline** кампании `POST /campaigns:inline` в одной PostgreSQL transaction
создаёт кампанию с 1–100 `user_id`. Если передан будущий `scheduled_at`, она
сразу переходит в `SCHEDULED`; если поле отсутствует — сразу в `STARTING` и
будет запущена ближайшим циклом scheduler. Она не наблюдается в `DRAFT`, не
принимает recipient batch-ей и не имеет отдельной операции `start`.

Для обоих режимов далее выполняется общий fencing lifecycle:

4. В момент фактического запуска scheduler под блокировкой строки кампании
   создаёт новый UUID `run_id`, сохраняет его вместе со статусом `STARTING` и
   `run_attempted_at = now()`, затем публикует
   `CampaignRunRequested(campaign_id, run_id)` через idempotent producer с
   `acks=all`: в `pushkin.campaign.batched.run` для batched-кампании или
   `pushkin.campaign.inline.run` для inline-кампании.
5. Kafka coordinator блокирует строку кампании и проверяет совпадение `run_id`.
   Для совпадающего run он переводит `STARTING` в `STARTED`; если статус уже
   `STARTED` с тем же `run_id`, это recovery повторной обработки того же Kafka
   record, а не причина пропустить fan-out. Для `FAILED` или чужого `run_id`
   coordinator не создаёт work и коммитит input offset.
6. После фиксации `STARTED` coordinator в Kafka transaction публикует
   `CampaignRunStarted(source_batches_total)` в `pushkin.campaign.progress`,
   соответствующий fan-out work и offset входного сообщения. Batched coordinator
   получает ID source batch-ей и публикует один work на batch; inline coordinator
   публикует один `InlineCampaignFanout` с неизменяемыми recipients и
   `source_batches_total = 1`. Если pod падает до Kafka commit, outputs не видны
   consumer-ам с `read_committed`, offset не продвигается, а повтор того же run
   заново создаёт тот же набор work. Если commit произошёл, видны одновременно
   и work, и offset.
7. Дальнейший streaming pipeline завершает кампанию только после terminal
   результатов всех delivery items.

Контракт интегратора для batched-кампании: он вызывает `start` только после
успешного подтверждения загрузки всех получателей в REST API. Это подтверждение
означает устойчивую запись в Pushkin; после `start` новые batch-и отклоняются.

Scheduler выбирает due `SCHEDULED` кампании и `STARTING`, для которых
`run_attempted_at IS NULL` либо оно старше заданного timeout. Под DB lock он
повторно проверяет условие и
создаёт новый `run_id` вместе с новым `run_attempted_at`. Старые
Kafka-сообщения не проходят проверку fencing и отбрасываются. Как только
coordinator зафиксировал `STARTED`, scheduler больше не создаёт новый run:
повторяется только тот же Kafka record, пока его transaction не закоммитит
fan-out work и input offset. `started_at` фиксируется coordinator-ом при
успешном переходе в `STARTED`. При неизвестном результате publish scheduler не
публикует старый `run_id` вручную: recovery создаёт новый fencing run.
Повторный запуск завершённой кампании выполняется её клонированием.

## 7. Внешние интерфейсы

REST предназначен для небольших потоков и простых интеграций:

```text
POST /api/v1/campaigns
POST /api/v1/campaigns:inline
POST /api/v1/campaigns/{campaign_id}/recipients:batch
POST /api/v1/campaigns/{campaign_id}/start
GET  /api/v1/campaigns/{campaign_id}
POST /api/v1/push-installations:register
```

В v1 общая идемпотентность REST-запросов не реализуется. Интегратор не должен
повторно загружать один recipient batch после получения успешного ответа.
Транзакционные уведомления не входят в v1.
`POST /campaigns` принимает `channel_key`, priority и push payload, а в ответ
возвращает внутренний UUID `campaign_id`.
`POST /campaigns/{campaign_id}/start` принимает опциональный `scheduled_at`:
будущее время планирует запуск, отсутствие поля запускает кампанию при ближайшем
цикле scheduler.
`POST /campaigns:inline` создаёт небольшую кампанию с неизменяемым списком от
1 до 100 `user_id`. Необязательный будущий `scheduled_at` переводит её в
`SCHEDULED`; отсутствие поля переводит её сразу в `STARTING`. Такая кампания не
бывает `DRAFT`, не принимает source batch-ей и не требует отдельного `start`
запроса.
Первая версия использует tenant-scoped API key: ключ передаётся в заголовке,
проверяется только по защищённому хешу и может быть отозван/ротирован. API key
определяет `tenant_id`; клиент не вправе подставлять произвольный tenant в теле
запроса.

### 7.1. Push payload первой версии

Интегрирующий сервис передаёт уже финальный push payload; шаблоны и серверная
персонализация не входят в v1. Контракт содержит только переносимые поля:

```json
{
  "notification": { "title": "…", "body": "…", "image": "https://…" },
  "data": { "type": "balance_reminder", "deep_link": "myapp://balance" }
}
```

`title` и `body` предназначены для отображения ОС, `image` опционален и задаётся
URL, а `data` — объект с данными для клиентского приложения. Push payload валидируется
по размеру до постановки кампании в очередь. Низкоуровневые platform overrides
FCM в v1 не предоставляются.

В v1 внешние Kafka-команды не поддерживаются: создание, наполнение аудитории и
запуск кампании выполняются только через REST. Kafka остаётся
внутренним data plane Pushkin и источником внешних событий user service.

## 8. Внутренний pipeline доставки

```text
PostgreSQL batched campaign + source batches
  → Kafka: pushkin.campaign.batched.run → pushkin.campaign.batched.source-batch.fanout
  → fan-out worker: user_id → active push installations
  → Kafka: один delivery work record на push installation
  → delivery worker → FCM
  → Kafka: retry work ИЛИ completed delivery result
  → stateful aggregator → compacted pushkin.campaign.stats → PostgreSQL projection

PostgreSQL inline campaign (1..100 user_id)
  → Kafka: pushkin.campaign.inline.run → pushkin.campaign.inline.fanout
  → inline fan-out worker: user_id → active push installations
  → Kafka: один delivery work record на push installation
```

Kafka является data plane и источником истины для logical work и её итогов.
PostgreSQL не получает update на каждую delivery attempt. Все internal consumers
работают с `isolation.level=read_committed`; producer workers используют
idempotent/transactional producer. Для всех операций вида «consume → produce →
commit offset» transaction должна быть короче `transaction.timeout.ms`.
Каждый internal Kafka record сериализуется в versioned envelope с полями
`type`, `schema_version` и `payload`; consumer декодирует envelope через общий
registry и application service явно проверяет ожидаемый тип сообщения.
Delivery worker открывает Kafka transaction после получения ответов FCM:
внешние сетевые вызовы намеренно не входят в Kafka transaction и при падении
могут быть повторены.

Очереди разделены по приоритетам. Tenant quotas проверяются в Redis, а свободная
pod capacity используется активными Channel без статического per-provider cap.
KEDA масштабирует stateless workers по consumer lag; полезный параллелизм
ограничивают число records в транзакции и pod semaphore.

В v1 `critical`, `high` и `normal` topics одного Channel обрабатываются
независимо и параллельно отдельными consumer groups. Приоритет выделяет
изолированное пространство backlog и consumer capacity для каждого класса, но
не гарантирует, что `critical` work будет выбран или доставлен раньше `normal`.
Он также не обещает порядок показа на устройстве:

| Приоритет | Назначение | Пример |
| --- | --- | --- |
| `critical` | Срочное важное событие; в v1 получает отдельный topic и consumer group. | Подозрительная активность, критичная блокировка номера. |
| `high` | Важное пользовательское, но не аварийное уведомление. | Скоро отключится услуга, требуется пополнение баланса. |
| `normal` | Обычные продуктовые и сервисные сообщения, включая кампании. | Изменился статус заявки, уведомление о программе или розыгрыше. |

Priority является полем кампании и в v1 задаётся интегрирующим сервисом без
ограничения со стороны Channel. Политику доступа к `critical` можно добавить
позже на уровне API key/ролей, если это потребуется tenant-у.

`pushkin.campaign.batched.source-batch.fanout` содержит ссылку на source batch в PostgreSQL. Fan-out worker
разрешает до 5 тыс. `user_id` в push installations через Redis/PostgreSQL и
создаёт один delivery work record на каждый target. У record есть стабильный
`delivery_id`; payload, token и provider parameters в него не дублируются. В
последней Kafka transaction fan-out worker также публикует в `pushkin.campaign.progress`
`SourceBatchFanoutCompleted(source_batch_id, delivery_count)` и коммитит offset
`pushkin.campaign.batched.source-batch.fanout`.

`pushkin.campaign.inline.fanout` содержит `campaign_id` и неизменяемый список
inline recipients. Worker читает до 100 таких records за раз, раскрывает каждый
в active push installations, публикует delivery work и
`InlineCampaignFanoutCompleted(delivery_count)` в одной Kafka transaction. Для
прогресса inline-кампания имеет ровно один логический source batch: coordinator
публикует `CampaignRunStarted(source_batches_total = 1)`, а её fan-out completion
увеличивает `source_batches_fanned_out` до одного.

Delivery work содержит только `delivery_id`, `campaign_id`, `tenant_id`,
`channel_id`, `push_installation_id`, `priority` и `retry_attempt`. Это компактный
контракт из ID и enum-ов; payload, token и provider parameters в него не
дублируются. Topic определяет `channel_id`, а `priority` должен совпадать с
сегментом имени topic. Worker разрешает Provider через mobile application,
связанное с push installation, и проверяет, что приложение активно. Payload
кампании и channel/application/provider config загружаются из process-local bounded
LRU/TTL cache, при miss — из Redis и затем PostgreSQL. Token разрешается по
`push_installation_id`, чтобы перед отправкой использовать его актуальную версию.
Cache не
является источником правды, имеет ограничение размера и TTL; очистка всей map
одним действием не используется.

Delivery worker читает до 100 individual delivery records, запускает их provider
calls параллельно и ограничивает общее число in-flight calls на pod semaphore-ом.
Перед захватом semaphore он проверяет distributed Provider limiter в
Redis: work заблокированного provider не занимает HTTP slot. После получения всех
outcome worker открывает одну Kafka transaction, публикует individual `RetryWork`
для work, требующего повторной попытки, и не более одного
`CampaignProgressDelta` на каждую затронутую кампанию. Затем он коммитит только
непрерывные offsets каждой partition. Delta содержит terminal результаты всей
processing group. Падение до commit отменяет output records и приводит к
повторной обработке input records. Внешний FCM-вызов всё равно может
повториться: это допустимая at-least-once доставка, но не двойной logical result
в Kafka. `accepted` означает «провайдер принял запрос», а не доказанный показ
уведомления пользователю.

Минимальные внутренние topics:

| Topic | Key | Назначение |
| --- | --- | --- |
| `pushkin.campaign.batched.run` | `campaign_id` | Запуск batched-кампании с `run_id` fencing. |
| `pushkin.campaign.batched.source-batch.fanout` | `source_batch_id` | Fan-out сохранённой аудитории в push installations. |
| `pushkin.campaign.inline.run` | `campaign_id` | Запуск inline-кампании с `run_id` fencing. |
| `pushkin.campaign.inline.fanout` | `campaign_id` | Fan-out до 100 неизменяемых recipients inline-кампании. |
| `pushkin.delivery.{priority}.{channel_id}` | `delivery_id` | Один work record на push installation; отдельный topic на Channel. |
| `pushkin.retry.{bucket}.{channel_id}` | `delivery_id` | Отложенные повторные попытки individual delivery work. |
| `pushkin.campaign.progress` | `campaign_id` | Markers fan-out и агрегированные terminal deltas для агрегатора. |
| `pushkin.campaign.stats` | `campaign_id` | Compacted абсолютный snapshot прогресса. |

События `CampaignRunStarted`, `SourceBatchFanoutCompleted`,
`InlineCampaignFanoutCompleted` и
`CampaignProgressDelta` публикуются в `pushkin.campaign.progress` с key
`campaign_id`. Они содержат соответственно число логических входных наборов
получателей, число созданных delivery items для одного batched source batch-а
или единственного inline input-а и суммы terminal outcomes одной Kafka
transaction (`accepted_delta`, `failed_delta`).

### 8.1. Разделение delivery и aggregation keys

Delivery pipeline и aggregation pipeline намеренно используют разные ключи.

```text
pushkin.delivery.{priority}.{channel_id}: key = delivery_id
  → individual delivery items одной кампании равномерно распределяются по partitions
  → Channel изолирован по backlog и приоритетам; лимиты provider-ов координирует Redis

pushkin.campaign.progress / pushkin.campaign.stats: key = campaign_id
  → все progress-события одной кампании находятся в одной partition
  → один aggregator worker владеет её счётчиками и состоянием
```

Так Pushkin сохраняет максимальный параллелизм в горячем пути доставки и
обходится без distributed locks/CAS при агрегации итогов кампании. Topics
`pushkin.campaign.progress` и `pushkin.campaign.stats` должны иметь одинаковое число partitions
и один алгоритм партиционирования по `campaign_id`, чтобы новый aggregator мог
восстановить свою локальную map из соответствующей partition compacted state.

### 8.2. Stateful campaign aggregator

Агрегатор — отдельная Go consumer group. Для каждой назначенной partition `p`
он до обработки новых progress-событий читает `pushkin.campaign.stats[p]` от начала до
текущего конца и сохраняет последний по Kafka offset snapshot для каждого
`campaign_id`, восстанавливая локальную map:

```text
campaign_id → {
  source_batches_total,
  source_batches_fanned_out,
  delivery_total,
  delivery_processed,
  total_count,
  delivery_accepted_count,
  delivery_failed_count
}
```

Затем worker читает `pushkin.campaign.progress[p]` с последнего закоммиченного offset.
Он может обработать небольшой набор событий (например, до 100–1000), применить
их к pending-копии локальной map и открыть Kafka transaction:

1. Опубликовать по одному полному `CampaignStatsSnapshot` для каждой изменённой
   кампании в `pushkin.campaign.stats`.
2. Добавить offsets прочитанных `pushkin.campaign.progress` records в ту же transaction.
3. Commit transaction и только после успеха применить pending-копию к map в
   памяти.

Если worker падает до commit, downstream consumers с `read_committed` не увидят
snapshots, offsets не продвинутся, а новый владелец partition восстановит
предыдущее состояние из `pushkin.campaign.stats` и повторит те же progress-события.
Consumer group гарантирует, что одну partition и, следовательно, одну кампанию
в момент времени обрабатывает лишь один aggregator worker; Kafka transaction
атомарно фиксирует snapshot состояния и offset входных событий.

### 8.3. Providers и rate limits

`Provider` — конкретная конфигурация tenant-а, а `ProviderType` — тип адаптера
(`fcm`, `twilio`, `ses` и т. д.). В v1 Channel типа `mobile_push` принадлежит
одному Provider типа `fcm` и выбирает одно или несколько его MobileApplication.
Для каждого активного
Channel создаются delivery topics и consumer groups
`pushkin.delivery.{priority}.{channel_id}`; все pod-ы worker role являются
членами соответствующей group. В v1 все три priority groups запускаются и
обрабатываются параллельно. Topic isolation упрощает управление backlog по
Channel, но не заменяет распределённое ограничение квоты между pod-ами.

При readiness каждый pod через Kafka Admin API идемпотентно гарантирует
фиксированные system topics. Перед запуском supervisor-а конкретного Channel
тот же provisioner идемпотентно гарантирует его три delivery и три retry topic-а.
В v1 Pushkin создаёт все internal topics с фиксированными четырьмя partitions.
Это часть topology: изменение числа partitions требует координированной
миграции topic-ов, а не изменения environment variable. Replication factor
остаётся политикой Kafka cluster; Pushkin задаёт cleanup policy и retention из
runtime configuration.

Один channel provisioning worker на pod с настраиваемым interval читает
активные Channel с доступным tenant и Provider из PostgreSQL и сверяет их с локальным набором
channel workers. В v1 новый Channel запускает отдельный supervisor с тремя delivery
и тремя retry loops; отключённый Channel, tenant или Provider отменяет этот
supervisor через context. Kafka распределяет partitions каждого topic между
pod-ами через соответствующую consumer group.

После v1 Channel сможет иметь несколько допустимых Provider-ов и policy
маршрутизации (`primary`, `weighted_split`, `fallback`). Delivery worker будет
выбирать кандидата при обработке record-а: попытка получить permit для Provider
в Redis одновременно проверяет лимит и расходует token, поэтому отдельная
проверка лимита до отправки не создаёт TOCTOU. В v1 policy не настраивается,
а единственный Provider выбирается всегда.

Redis хранит token buckets на двух обязательных уровнях:

```text
provider:{provider_id}  — QPS/burst конкретной provider configuration
tenant:{tenant_id}      — общая квота клиента Pushkin
```

Перед provider calls worker берёт permits из обоих buckets одной Lua-операцией:
она пополняет buckets плавно по времени Redis, проверяет доступную ёмкость и
списывает одинаковое число permits только если хватает в обоих scopes. Provider
хранит `rate_limit_qps` и `rate_limit_burst`; tenant quota в минуту переводится
в rate/sec внутри limiter-а. Token bucket пополняется непрерывно, поэтому нет
burst в начале календарной минуты. Topic конкретного Channel принадлежит одному
tenant, поэтому один poll не смешивает tenant-ов. При нехватке permits worker
сначала делает seek каждой затронутой partition к минимальному offset из
непринятой processing group, затем pause-ит весь delivery topic своего Channel,
продолжая heartbeat/poll loop consumer group, и ждёт пополнения bucket-а. Он не
создаёт retry только из-за обычного ожидания permit.

Ответ provider `429`/`Retry-After` устанавливает shared Redis `blocked_until`
для Provider. Worker больше не выбирает заблокированный Provider; work, для
которого в v1 нет альтернативы, переводится в retry с соответствующим `due_at`.
Общий pod semaphore ограничивает сумму provider calls, но не вводит
искусственный per-provider concurrency cap: свободная ёмкость доступна активному
Provider. Если для `critical` понадобится жёсткое SLA, позже добавляется reserved
capacity/fair scheduler.

## 9. Ретраи и завершение

Для ретраев используются отдельные time-bucket topics
`pushkin.retry.{1m|5m|30m}.{channel_id}`. `RetryWork` сохраняет тот же
`delivery_id`, неизменяемые routing fields (`tenant_id`, `channel_id`,
`push_installation_id`, `priority`), номер `retry_attempt` и `due_at`. Retry worker
читает ровно один record из retry topic-а за обработку. Если `due_at` ещё не
наступил, он делает seek назад на offset этого record-а, затем pause-ит
назначенную partition до этого времени, продолжая heartbeat/poll loop consumer
group. Это не позволяет Kafka client-у локально перескочить отложенный record до
перезапуска consumer-а. После `due_at` worker выполняет новую provider attempt прямо из retry topic-а:
в Kafka transaction публикует terminal result либо следующий `RetryWork` и
коммитит input offset. Due retry record не возвращается в хвост delivery topic.
Три retry-волны и их buckets фиксированы в v1 и являются частью topology, а не
env-конфигурацией.
Политика выбора бакета задаётся по классу ошибки:

- временные network/provider ошибки: exponential backoff + jitter, ограниченное
  число попыток;
- quota/`429`/`Retry-After`: backoff по ответу провайдера и снижение темпа;
- invalid/unregistered token: деактивация push installation без ретрая;
- невалидный payload/контракт: terminal failure;
- исчерпанные попытки (в v1: максимум 3): terminal failure и
  `CampaignProgressDelta`.

FCM не предоставляет универсальную гарантию дедупликации. `collapse_key`/
collapse ID — это замена устаревающих уведомлений, а не защита от дубликатов.
Клиентское приложение может дополнительно подавлять повторный показ.

Terminal outcomes одного processing group суммируются в один
`CampaignProgressDelta` на каждую затронутую кампанию. Delta является output
транзакционной цепочки work → retry/work → progress; при падении до Kafka commit
она не видна и входные work records переигрываются. Stateful campaign aggregator
применяет только такие deltas и
публикует абсолютный `CampaignStatsSnapshot` в compacted topic
`pushkin.campaign.stats`. Каждый batched fan-out worker вместе со своими delivery
work records публикует `SourceBatchFanoutCompleted(source_batch_id, delivery_count)`;
inline fan-out worker публикует `InlineCampaignFanoutCompleted(delivery_count)`.
Aggregator знает число логических входных наборов запуска, поэтому после
получения всех таких markers получает точное число delivery items. Кампания `COMPLETED`, когда
`source_batches_fanned_out == source_batches_total` и
`delivery_processed == delivery_total`. PostgreSQL принимает
snapshot синхронно и в Kafka log order. Kafka lag сам по себе не используется
как критерий конца кампании.

## 10. Хранилища, лимиты и cache

- **PostgreSQL**: control plane: метаданные, source batches batched-кампаний, push-installation registry
  и последняя проекция `pushkin.campaign.stats`. Он не хранит
  delivery work и не обновляет счётчики после каждой отправки.
- **Redis**: distributed tenant/provider rate limiting. Он не является
  источником правды и не используется как единственное хранилище отложенных
  задач. Cache-aside для push installations отложен и не участвует в v1
  correctness.
- **Kafka**: source/delivery work, ретраи, terminal progress deltas, changelog state,
  compacted campaign snapshots. Это source of truth для
  logical progress кампании.

### 10.1. Runtime limits

Все limits задаются environment variables и могут быть переопределены в Helm
values. Начальные значения предназначены для безопасного старта и уточняются
нагрузочным тестированием.

| Переменная | Значение по умолчанию | Назначение |
| --- | ---: | --- |
| `PUSHKIN_SOURCE_BATCH_MAX_SIZE` | `5000` | Максимум `user_id` в batched source batch. |
| `PUSHKIN_DELIVERY_PROCESSING_BATCH_SIZE` | `100` | Максимум individual delivery records в одном poll и одной Kafka transaction. |
| `PUSHKIN_DELIVERY_MAX_IN_FLIGHT` | `500` | Максимум одновременных provider calls на одном pod. |
| `PUSHKIN_CAMPAIGN_STARTING_TIMEOUT` | `1m` | После этого времени scheduler восстанавливает `STARTING` кампанию без publish attempt. |
| `PUSHKIN_SCHEDULER_INTERVAL` | `1s` | Интервал polling scheduler-а. |
| `PUSHKIN_SCHEDULER_BATCH_SIZE` | `100` | Максимум due campaign за один scheduler run. |
| `PUSHKIN_CAMPAIGN_PROGRESS_BATCH_SIZE` | `100` | Максимум progress records за одну aggregation Kafka transaction. |
| `PUSHKIN_CAMPAIGN_STATS_BATCH_SIZE` | `1000` | Максимум snapshots за один projection run. |
| `PUSHKIN_INLINE_CAMPAIGN_RUN_BATCH_SIZE` | `100` | Максимум inline run records за один coordinator run. |
| `PUSHKIN_INLINE_CAMPAIGN_FANOUT_BATCH_SIZE` | `100` | Максимум inline fanout records за одну Kafka transaction. |
| `PUSHKIN_CHANNEL_PROVISIONING_INTERVAL` | `1s` | Частота reconcile runnable Channel и локальных channel workers на pod. |
| `PUSHKIN_CREDENTIALS_CIPHER_KEY_BASE64` | — | Обязательный AES-256-GCM ключ в base64; хранится вне PostgreSQL. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Необязательный OTLP gRPC endpoint для metrics export. Без него telemetry no-op. |
| `PUSHKIN_TELEMETRY_METRICS_EXPORT_INTERVAL` | `10s` | Интервал periodic OTLP metrics export. |

`PUSHKIN_DELIVERY_MAX_IN_FLIGHT=500` — безопасный default, а не жёсткая
граница. Значение выбирается по provider latency, error rate и нагрузочным
тестам; benchmark подтвердил, что конкретная среда может требовать большего
числа одновременных calls.

### 10.2. Retention и очистка данных

В v1 Pushkin конфигурирует только retention Kafka work/progress topics. Значения
ниже позволяют расследовать инцидент и повторно проиграть недавний pipeline, но
не превращают Kafka в бессрочный архив.

| Данные | Default | Политика |
| --- | ---: | --- |
| `pushkin.campaign.batched.*`, `pushkin.campaign.inline.*`, `pushkin.delivery.*`, `pushkin.retry.*` | 7 дней | Обычный `delete` retention. Максимальная задержка и все retry-волны должны быть существенно короче этого срока; мониторинг обязан алертить до истечения retention при lag. |
| `pushkin.campaign.progress` | 14 дней | Обычный `delete` retention для краткого replay/диагностики агрегатора. |
| `pushkin.campaign.stats` | compact + 14 дней | Topic использует `cleanup.policy=compact,delete` и тот же retention, что `pushkin.campaign.progress`. Compaction сохраняет последний snapshot кампании внутри окна восстановления, после которого state удаляется вместе с исходным progress log. |
| Source batches batched-кампаний в PostgreSQL | без автоматической очистки | v1 сохраняет batches до появления отдельной безопасной cleanup policy. Inline recipients живут вместе с записью кампании и отдельной cleanup policy не требуют. |

Эквивалентные defaults задаются переменными `PUSHKIN_KAFKA_WORK_RETENTION`,
`PUSHKIN_KAFKA_PROGRESS_RETENTION`. Перед сокращением любого срока нужно
проверить максимальную допустимую задержку кампании, retry schedule и SLO на
восстановление после инцидента.

### 10.3. Push-installation cache в Redis

Redis cache push installations пока не реализован. В v1 PostgreSQL остаётся
единственным resolver-ом active installations и актуальных tokens; Redis
используется только для tenant/provider rate limiting. Cache-aside слой ниже является
проектом последующей оптимизации и не должен быть предпосылкой корректности
delivery pipeline.

Нормализованная таблица `push_installations` в PostgreSQL остаётся авторитетной.
По ней должен быть индекс для выборки активных installations по `(tenant_id,
mobile_application_id, user_id)`. PostgreSQL достаточно для старта и как fallback, но
на пике 30–40 тыс. delivery/sec постоянное разрешение пользователей в токены
создаст большой read-трафик и конкуренцию с транзакционными операциями. Redis
снимает эту нагрузку с горячего пути.

Предлагаемая схема:

1. Fan-out worker получает applications Channel и читает ключи
   `push-installations:{tenant}:{application}:{user}`.
2. При cache hit получает только активные installations с версией/временем
   обновления и создаёт delivery work.
3. При miss запрашивает PostgreSQL, возвращает результат и кладёт его в Redis с
   ограниченным TTL (например, 5–15 минут, уточняется нагрузочным тестом).
4. Delivery worker разрешает свой `push_installation_id` через отдельный ключ
   `push-installation:{push_installation_id}`. Так Kafka record не содержит token, а отправка берёт
   актуальную версию устройства.
5. Регистрация, смена или деактивация токена транзакционно меняет PostgreSQL и
   инвалидирует/обновляет оба ключа Redis. TTL остаётся второй линией защиты от
   рассинхронизации.
6. Ответ FCM `unregistered`/`invalid token` немедленно деактивирует installation в
   PostgreSQL и инвалидирует соответствующие cache keys.

Кэш должен переживать промахи и полную очистку без потери уведомлений: workers
всегда умеют сходить в PostgreSQL. Значения с токенами шифруются либо хранятся в
изолированном Redis с TLS/ACL и шифрованием диска; токены не попадают в метрики
и сырые delivery-логи.

Payload кампании и channel/application/provider config кэшируются отдельно от
push-installation records: process-local LRU использует ключи
`campaign:{campaign_id}:{revision}`, `channel:{channel_id}:{revision}`,
`mobile-application:{application_id}:{revision}` и
`provider:{provider_id}:{revision}`, а Redis — соответствующие versioned
snapshots. При старте кампании фиксируется её Channel и набор приложений; новое
приложение, добавленное в Channel позже, не меняет уже запущенную кампанию.
Отключение application или его Provider применяется и к уже созданному work:
worker завершает такую delivery без вызова FCM. Ротация credentials внутри того
же Provider публикует новую revision и инвалидирует старый key.
Это позволяет маленькому delivery work record не обращаться к PostgreSQL на
каждую отправку.

## 11. Безопасность и эксплуатация

- FCM service-account JSON шифруется AES-256-GCM до сохранения в PostgreSQL.
  Master key приходит через environment variable и не хранится в PostgreSQL.
  Kubernetes Secret либо внешний secret manager будут production-механизмом
  поставки этой переменной при добавлении Kubernetes deployment.
- Kubernetes deployment (Helm chart, Secrets, network policies и autoscaling)
  будет добавлен до production release; локальные разработка и benchmark пока
  используют Docker Compose и Terraform/Ansible соответственно.
- В v1 доступ tenant-а даёт один API key; более детальный RBAC для создания,
  запуска и просмотра отчётов — последующее расширение.
- Durable audit trail control-plane действий пока не реализован и требует
  отдельной модели retention и доступа.
- Сетевые политики, TLS, ротация секретов, encryption at rest для токенов.
- Tenant/provider rate limits поддерживаются Redis token bucket-ом. Circuit
  breakers требуют отдельного проектирования.
- Резервное копирование PostgreSQL, retention-политики Kafka,
  восстановление consumer offsets и регулярные disaster-recovery проверки.

## 12. Отложено после v1

В первой версии push payload ограничен `title`, `body`, `data` и опциональным
`image`; шаблоны, персонализация, `ttl_seconds`, collapse key и platform
overrides не поддерживаются. Они могут быть добавлены отдельной версией
контракта без изменения delivery pipeline.

Поддержка SMS и e-mail потребует заменить v1 `PushPayload` кампании на
типизированный content, соответствующий `channel_type`: `PushPayload`,
`SMSPayload` или `EmailPayload`. Campaign сохранит общие lifecycle, аудиторию,
priority и Channel, а content будет валидироваться для конкретного Channel.
`DeliveryWork` останется компактной ссылкой без content и фактического адреса,
но вместо v1 `push_installation_id` получит `target_type` и `target_id`.
`target_id` будет ссылаться на конкретную запись установки, номера или e-mail,
без обязательной общей таблицы адресов. Fan-out и provider adapter разрешат
target по его типу. Это позволяет добавить новые каналы без дублирования
pipeline и без хранения токенов/адресов в Kafka record.

Source batch также станет типизированным: у каждого однородного batch появится
`recipient_type` (`user_id`, `phone_number` или `email`) и список строковых
значений одного типа. `user_id` останется основным корпоративным путём и будет
разрешаться через user projection; явные номера и e-mail позволят отправлять
адресные SMS/e-mail без привязки к пользователю. Тип задаётся на уровне batch,
а не отдельного элемента, чтобы fan-out выполнял один специализированный
resolver для всего чанка.

Retry topology в v1 намеренно фиксирована: три buckets с возрастающей задержкой
`1m → 5m → 30m` и затем terminal failure. При необходимости более гибкой
политики нужно отдельно спроектировать retry scheduler с произвольным `due_at`,
числом попыток и backoff policy; это не должно менять семантику уже
опубликованных retry records незаметно для работающей инсталляции.

Dead-letter queue появится после v1. До её добавления нужно детально разобрать
все нештатные пути pipeline: ошибки декодирования и несовместимый контракт,
poison records, ошибки бизнес-валидации, исчерпанные retries, сбои Kafka и
внешнего provider. Для каждого случая следует решить, должен ли work быть
повторён, считаться terminal failure, останавливать worker или попадать в DLQ.
Отдельно нужно определить цель DLQ — диагностика, ручной replay или оба
варианта, — безопасный состав origin metadata без токенов и payload, retention,
права доступа, alerts и процедуру разбора/replay. Только после этого можно
добавить `pushkin.dlq` и его транзакционную публикацию в pipeline.

Очистка source batches batched-кампаний появится после v1. Нужно отдельно определить retention,
audit requirements, batch size и защиту от удаления ещё активной кампании.

Approval workflow для массовых/marketing кампаний в v1 отсутствует. Если он
понадобится, его следует добавить как control-plane переходы состояний до
создания `CampaignRunRequested`, а не в горячий Kafka delivery path.

Отмена кампании появится после v1 как best-effort control-plane операция. Она
прекращает выпуск нового work и позволяет workers пропускать ещё не отправленные
delivery, но не отзывает уже начатые или принятые FCM вызовы. Для этого потребуется
отдельный durable control state и его распространение между всеми delivery workers.

Metrics-only observability уже доступна как необязательный OTLP export.
Локальный Compose overlay содержит Collector, Grafana LGTM и exporters для
PostgreSQL, Redis и Kafka. Production dashboards, alert rules, logs, traces и
SLO остаются отдельной работой; Grafana Cloud не является обязательной
зависимостью.

Текущий topic-per-Channel подход рассчитан на ограниченное число активных
channels в одной инсталляции. При необходимости поддерживать сотни/тысячи
channels нужно отдельно спроектировать multiplexed delivery topics и
планировщик, который сохраняет изоляцию quota/backlog без consumer group на
каждый Channel.

При появлении требования к строгому приоритету нужно отдельно спроектировать
priority scheduler: он должен выбирать delivery work `critical → high → normal`
при ограниченной общей мощности, определить starvation protection для `normal`
и при необходимости reserved capacity. Нельзя выдавать такую гарантию только
за счёт текущего разделения topic-ов.

Аналитика delivery attempts и ClickHouse: добавить append-only topic
`pushkin.delivery.events` с key=`delivery_id` и версионированный record
`DeliveryEventV1`: `type` (`accepted`, `failed`, `retried`), `delivery_id`,
`campaign_id`, `tenant_id`, `channel_id`, `retry_attempt`, `occurred_at` и
безопасный `failure_reason`. Delivery и retry workers будут публиковать event
в той же Kafka transaction, что terminal progress delta либо следующий retry.
Добавить отдельный Kafka consumer для batch ingest и ClickHouse schema с
настраиваемым TTL. Event не содержит token, provider secret или полный payload.
До этого агрегированный progress кампании в PostgreSQL остаётся единственным
отчётным состоянием v1.

Идемпотентность внешних запросов: спроектировать durable `Idempotency-Key` для
операций, где повтор действительно создаёт новый эффект, прежде всего для
загрузки recipient batch-ей и будущего `POST /api/v1/notifications`. Запись
ключа, hash запроса и scalar result должна сохраняться в одной PostgreSQL
transaction с изменяемыми моделями; один ключ с другим запросом даёт conflict.

Пользовательский notification inbox: добавить отдельный application port с
двумя взаимозаменяемыми адаптерами — PostgreSQL (отдельная append-only таблица
с retention/партиционированием) и Cassandra/совместимый wide-column store.
Выбор реализации конфигурируется как `disabled`, `postgres` или `cassandra`;
inbox не является обязательной зависимостью Pushkin. В обоих вариантах хранится
одно логическое принятое уведомление на пользователя, а не delivery attempt на
устройство; основной запрос — newest-first с cursor pagination. Нужно определить
user-facing API и модель авторизации, момент создания записи после provider
acceptance, `notification_id` для дедупликации, TTL/правила удаления и отдельный
Kafka projection consumer. Будущая техническая аналитика delivery attempts не
заменяет inbox.
