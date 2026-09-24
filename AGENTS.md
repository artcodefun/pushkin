# Pushkin engineering rules

This file is the mandatory, self-contained engineering contract for changes in
this repository. It defines stable implementation conventions, not the current
product specification. Product behavior belongs in explicit domain code,
versioned contracts, and tests.

## Changing these rules

- Do not silently introduce a pattern that contradicts this file.
- Before an exception, explain its concrete reason and trade-off to the user.
- After approval, update this file in the same change so it remains current.
- Prefer the smallest change consistent with existing conventions. Do not add
  speculative abstractions for future features.

## Language and toolchain

- Use Go 1.27 or newer as declared by `go.mod`.
- Use the standard-library `uuid` package. Do not add another UUID library.
- Internal entity IDs use `uuid.UUID`, normally UUID v7. Domain factories or
  domain behavior create them; callers do not provide them for new entities.
- Rehydration is added only with a persistence adapter that needs it and must
  preserve existing internal IDs.
- External IDs, idempotency keys, and deduplication keys remain opaque strings.
- Run `gofmt` on changed Go files. Code must pass `go test -race ./...` and
  `go vet ./...` before completion.

## Dependency direction

Allowed flow:

```text
HTTP and external-Kafka interfaces ---> application ---> domain
internal Kafka workers ----------------> application ---> domain
outbound infrastructure ---------------> application ports + domain
bootstrap constructs concrete objects and owns lifecycle
```

- `internal/domain` imports only the standard library. It never imports
  application, contracts, adapters, database code, Kafka clients, Redis, HTTP,
  provider SDKs, logging, metrics, or environment configuration.
- `internal/application` may import domain, its own ports, and private
  `internal/contracts/kafka` records used by the internal pipeline. It must not
  import concrete infrastructure, public transport DTOs, or a Kafka client.
- `internal/interfaces` and runtime workers translate a transport into finite
  application calls. They may import application, domain, and contracts.
- `internal/infrastructure` implements outbound capabilities. It may import
  application ports and domain types but does not make business decisions.
- `internal/bootstrap` is the composition root. It constructs dependencies and
  starts/stops processes; it contains no business rules.
- Circular dependencies and service-locator/global-container patterns are
  forbidden.

## Package structure

Keep the v1 layout:

```text
api/http/v1/                  public OpenAPI contract
api/kafka/v1/                 public external Kafka contracts
cmd/server/                   production executable
cmd/fake-fcm/                 test-only local fake provider executable
deploy/benchmarks/            disposable benchmark infrastructure and tools
internal/domain/              one domain package, split into focused files
internal/application/commands write use cases
internal/application/queries  read use cases and read models
internal/application/services finite background/pipeline use cases
internal/application/ports    outbound interfaces required by application
internal/contracts/kafka/     private versioned Kafka pipeline contracts
internal/interfaces/http/     primary HTTP adapter
internal/interfaces/kafka/    primary adapter for external Kafka events
internal/infrastructure/      PostgreSQL, Kafka, Redis, providers, analytics
  postgres/repo/              write repositories and transaction manager
    mappers/                  persisted rows to domain aggregate mapping
  postgres/readrepo/          read-model repositories
    mappers/                  persisted rows to query read-model mapping
internal/bootstrap/           construction, worker registration, lifecycle
```

- Do not split `internal/domain` into packages per entity while the project is
  one bounded context.
- Do not create generic `platform`, `common`, `utils`, or `helpers` packages.
- Root `api/` contains only contracts consumed by external systems. Internal
  Kafka records belong in `internal/contracts/kafka`.
- `cmd/fake-fcm` is the sole exception to the single production binary: it is
  used only by local Compose and test environments to exercise outbound HTTP
  delivery. It must never be deployed as a production role.
- Do not create further binaries or deployment roles without an explicit
  decision and a matching update to this file.
- A benchmark may contain a small, self-contained Go load generator under
  `deploy/benchmarks/<benchmark>/tools/`. It is invoked only by that
  benchmark's entrypoint, has no production role, and must use standard
  library HTTP clients unless a concrete measurement requires otherwise.

## Domain code

- Domain structs with behavior keep state private. Expose read-only getters and
  behavior methods; callers do not mutate aggregate state directly.
- Constructors validate invariants knowable from their inputs and return typed
  errors when unsafe state would otherwise be created.
- Product invariants are expressed in constructors, aggregate methods, or a
  domain service and covered by tests. Do not rely on comments or application
  code alone to preserve an invariant.
- Use a domain service when a rule needs several aggregates or value objects and
  does not naturally belong to one aggregate. A domain service is an explicit
  `*Service` type with no repository, transport, or infrastructure dependency.
- Copy maps, slices, and pointer values on input/output. Callers must not
  mutate domain state through aliases.
- Retriable domain behavior is explicitly idempotent where the workflow needs
  it. Invalid transitions return typed/sentinel errors usable with `errors.Is`.
- Domain code never returns HTTP status codes or Kafka actions.
- Pass a timestamp only when it participates in a domain rule. Do not add
  Clock/UUID ports or mutable package-level time providers.
- Do not introduce snapshots or rehydration DTOs before persistence requires
  them. Use explicit mapping; never reflection or private-field mutation.
- When persistence requires reconstitution, put its `Hydrate…Params` type and
  `Hydrate…` factory at the end of the domain file so normal creation and
  business behavior remain the primary reading path.
- Kafka records are not domain events. Do not add a domain-event framework or
  event producer to aggregates without an explicitly agreed use case.

## Application code

- A command or application service method represents one finite use case. Its
  inputs are application/domain types, never raw HTTP requests, Kafka records,
  SQL rows, or provider SDK responses.
- Commands coordinate repositories and transactions; aggregates and domain
  services own product rules and state transitions.
- Command results contain only scalars, identifiers, small immutable result
  values, and errors. They never return mutable domain aggregates.
- Queries return dedicated read models, never mutable domain aggregates.
- Read repositories return tenant-scoped read models directly. Their
  infrastructure implementations map storage rows or projections to read
  models without rehydrating domain aggregates; query services do not map
  aggregates into read models.
- `context.Context` is the first parameter of application and port operations
  that perform I/O. Domain methods do not accept context.
- Add a port only at a real external dependency or policy boundary. Do not add
  interfaces for private helpers, time, UUID generation, or every concrete type.
- Application code may request narrow semantic atomic operations through ports;
  it must not receive an unbounded database or Kafka client handle.
- Return enough typed information for the transport/runtime to decide whether
  to retry, commit, reject, or route work to a dead-letter path.
- Application-defined outcomes use sentinel errors from `internal/application`
  (`ErrNotFound`, `ErrNotAuthorized`, `ErrValidation`, `ErrConflict`, and
  `ErrUnavailable`).
  Application code may inspect or wrap a domain error returned by a domain
  operation, but it must not manufacture a new `domain.Err*` error for its own
  validation, authorization, or orchestration logic.
- Primary adapters map application errors before domain errors. Application
  errors are the more specific use-case contract; domain errors remain a
  fallback for unmodified domain-operation failures.

## Interfaces and runtime

- HTTP handlers authenticate, validate/decode DTOs, call one application use
  case, and map application errors to responses. They contain no repository or
  domain-transition logic.
- Public request/response schemas are defined in `api/http/v1` before or with
  handler changes. External Kafka contracts are versioned and backward
  compatible within a version.
- Internal Kafka pipeline loops live in `internal/bootstrap`, where their
  process lifecycle is composed with the rest of the executable. A worker owns
  cancellation, heartbeat/rebalance handling, and backoff; bootstrap starts and
  stops every worker. The corresponding application pipeline service owns
  polling, Kafka transactions, publishing, pause/resume, and offset commits
  through a narrow Kafka-aware port. This deliberate Kafka coupling exists to
  make the pipeline's consume-produce-offset atomicity explicit; it is not an
  attempt to abstract Kafka into a replaceable broker.
- Internal records use versioned types from `internal/contracts/kafka`; do not
  serialize domain aggregates directly.
- A provider network call can be repeated after a crash. Never claim
  exactly-once delivery to an external provider.

## Infrastructure and security

- Repository implementations use explicit mapping/reconstitution code. They do
  not use reflection or bypass domain validation.
- PostgreSQL adapters use `pgx/v5` and `sqlc`. Versioned schema changes are
  paired up/down SQL migrations under `internal/infrastructure/postgres/migrations`;
  query SQL lives under `internal/infrastructure/postgres/queries`; generated
  `gen/` code is regenerated with `sqlc generate` and never edited manually.
- Keep PostgreSQL write repositories in `postgres/repo` and read-model
  repositories in `postgres/readrepo`; do not combine them in one package.
- Keep write-side aggregate reconstitution mappers in `postgres/repo/mappers`
  and read-model mappers in `postgres/readrepo/mappers`; never use a read
  mapper to rehydrate a domain aggregate.
- Keep SQL readable without artificial vertical expansion: positional
  placeholder lists such as `($1, $2, $3)` stay on one line unless an expression
  or a line-length constraint makes a multiline form clearer.
- Name write query files `<entity>.sql` and their read-model counterparts
  `<entity>.read.sql`.
- Redis is cache/coordination, never the sole durable source of truth.
- Configuration is parsed and validated in bootstrap, then passed as typed
  values. Domain and application code do not read environment variables.
- Provider-specific statuses/errors are mapped at the adapter boundary before
  product decisions are made.
- Tokens, API keys, provider secrets, and full notification payloads must not
  appear in logs, metrics, traces, client-visible errors, or analytics events.
- Do not add a production dependency without a concrete use case; prefer the
  standard library when it provides the required semantics.

## Errors, naming, and style

- Wrap errors with `%w` when preserving their category. Do not inspect error
  message text for control flow.
- Use the product language already established in the package; avoid vague
  synonyms where a precise domain type exists.
- Keep comments for invariants, non-obvious concurrency semantics, and reasons;
  do not narrate straightforward code.
- Prefer explicit code over reflection, generic repositories, magic
  registration, and broad base abstractions.
- Do not introduce mutable package globals. Constants and immutable sentinel
  errors are allowed.

## Testing and definition of done

- Domain behavior and state transitions require table-driven unit tests.
- Every reproducible production bug fix starts with a regression test.
- Application tests use small handwritten fakes for ports. Mocks verify
  meaningful behavior, not implementation order.
- Infrastructure with concurrency or transactional semantics requires
  integration tests against the real backing service.
- PostgreSQL integration tests use the `integration` build tag. Run them with
  `go test -race -tags=integration ./internal/infrastructure/postgres`.
  They always start an isolated PostgreSQL Testcontainers instance.
- Kafka integration tests use the same tag. Run them with
  `go test -race -tags=integration ./internal/infrastructure/kafka`.
  They always start an isolated Kafka Testcontainers instance and explicitly
  create every topic they need; do not depend on broker auto-topic-creation.
- Redis integration tests use the same tag. Run them with
  `go test -race -tags=integration ./internal/infrastructure/redis`.
  They always start an isolated Redis Testcontainers instance; do not add a
  local Redis endpoint fallback.
- Tests must be deterministic. Do not use sleeps where channels, barriers, or
  controllable timestamps can express the condition.

A change is complete only when it follows these rules, includes proportionate
tests, passes formatting/test/vet checks, updates affected versioned contracts,
and introduces no secret, cache, generated artifact, or machine-specific file.
