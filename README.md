# workflow-world-surrealdb

SurrealDB-backed World implementation for Workflow SDK. This package stores workflow state in SurrealDB and uses `LIVE SELECT` to drive queue wake-ups and stream fan-out, with `SHOW CHANGES` as the recovery path after a live subscription reconnects.

## Compatibility

This package implements the Workflow SDK 5 World contract and declares its spec version with `mintedSpecVersion()` from `@workflow/world` (spec version 8 by default, 6 with `WORKFLOW_SEALED_LOG=0`). Use it with `workflow` 5.x. 0.1.x implements the Workflow SDK 4 contract and is rejected by `workflow` 5.

## Installation

```bash
npm install workflow-world-surrealdb
# or
pnpm add workflow-world-surrealdb
# or
yarn add workflow-world-surrealdb
```

## Usage

Set the target world:

```bash
export WORKFLOW_TARGET_WORLD="workflow-world-surrealdb"
```

### Configuration

```bash
export WORKFLOW_SURREAL_URL="ws://127.0.0.1:8000/rpc"
export WORKFLOW_SURREAL_NAMESPACE="workflow"
export WORKFLOW_SURREAL_DATABASE="workflow"

# Optional auth
export WORKFLOW_SURREAL_USERNAME="root"
export WORKFLOW_SURREAL_PASSWORD="root"
# or
export WORKFLOW_SURREAL_TOKEN="..."

# Optional queue tuning
export WORKFLOW_SURREAL_QUEUE_CONCURRENCY="10"
export WORKFLOW_SURREAL_QUEUE_LEASE_MS="300000"
export WORKFLOW_SURREAL_QUEUE_HEARTBEAT_MS="10000"
export WORKFLOW_SURREAL_QUEUE_LANE_SHARDS="16"
# Ignored since Workflow SDK 5, which delivers steps on the workflow queue
export WORKFLOW_SURREAL_STEP_QUEUE_LANE_SHARDS="16"
export WORKFLOW_SURREAL_WORKFLOW_QUEUE_LANE_SHARDS="16"
export WORKFLOW_SURREAL_LIVE_RECONNECT_DELAY_MS="1000"

# Optional local execution base URL override
export WORKFLOW_LOCAL_BASE_URL="http://127.0.0.1:3000"
```

### Local Docker Compose

A ready-to-use compose file is included at `packages/world-surreal/docker-compose.yml`:

```bash
cd packages/world-surreal
export SURREAL_DB_PASS="root"
docker compose up -d

export WORKFLOW_SURREAL_URL="ws://127.0.0.1:8000/rpc"
export WORKFLOW_SURREAL_NAMESPACE="workflow"
export WORKFLOW_SURREAL_DATABASE="workflow"
export WORKFLOW_SURREAL_USERNAME="root"
export WORKFLOW_SURREAL_PASSWORD="$SURREAL_DB_PASS"
```

This compose setup uses a named Docker volume plus `surrealkv:///data/workflow.db?versioned=true`, so the database survives container restarts.

### Programmatic usage

<!-- @skip-typecheck: incomplete code sample -->
```ts
import { createWorld } from 'workflow-world-surrealdb';

const world = createWorld({
  url: 'ws://127.0.0.1:8000/rpc',
  namespace: 'workflow',
  database: 'workflow',
  auth: {
    username: 'root',
    password: 'root',
  },
});
```

### Schema setup

This package lazily defines its tables on first use, but you can also create them explicitly:

```bash
pnpm exec workflow-surreal-setup
# or
pnpm --filter workflow-world-surrealdb db:push
```

## Design notes

- Workflow entities still follow the same event-sourced contract as the other worlds: `events.create()` is the only write entrypoint for run, step, hook, and wait state.
- Event ids are slot positions (`slotToEventId()` from `@workflow/world`): a run's events are numbered densely from 1. Each `events.create()` reads the run's highest slot, then applies its entity changes and appends its event at the next slot in one SurrealDB transaction. The event record key `${runId}:${eventId}` is the uniqueness constraint, so a concurrent writer that took the slot makes the transaction roll back, and the create is retried from fresh reads. A write that lands above the slot its writer expected (`eventCount + 1`) returns the events it skipped over.
- The schema is Surreal-first: tables are `SCHEMAFULL`, hot query paths are indexed explicitly, and hook uniqueness is enforced with `tokenHash` instead of an auxiliary lock table.
- Queue scheduling is live-first: workers subscribe to lane summary rows with `readyCount > 0`, and a short idle poll only refreshes due lanes and expired leases instead of scanning the whole jobs table each cycle.
- Queue retry timing now follows a Graphile-style exponential backoff for handler failures, with the delay computed inside SurrealDB functions so retry scheduling uses database time instead of process-local time.
- Queue lane summaries are maintained incrementally in SurrealDB so enqueue, claim, complete, and retry paths stay on single-row control-plane updates.
- Completed queue jobs are deleted after acknowledgement, keeping the hot queue table compact instead of accumulating `completed` rows.
- Queue recovery is changefeed-backed: after the live socket reconnects, the worker replays `SHOW CHANGES FOR TABLE workflow_queue_jobs` from the last seen timestamp before resubscribing.
- Stream reads are chunk-based: writers append chunk rows, readers consume an initial snapshot plus live chunk notifications.
- Opaque workflow payloads are stored as CBOR payloads, while native Surreal-friendly fields such as timestamps, statuses, and execution metadata remain directly queryable.

## Current limitations

- Runs created by 0.1.x of this package (Workflow SDK 4) use ULID event ids and cannot be replayed by Workflow SDK 5. Drain them on the old release before upgrading.
- SurrealDB 3 no longer creates a namespace or database on first use. Schema setup defines the configured ones when the user is allowed to; otherwise create them before starting the World.
- This package is currently optimized for the single-node Live Query model described in SurrealDB’s documentation.
- This package does not preserve any pre-Surreal legacy storage mode; it expects records created by the current `workflow-world-surrealdb` implementation.
