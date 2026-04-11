import type { SurrealRpcClient } from './rpc.js';
import { retrySurrealWrite } from './util.js';

export const schemaStatements = `DEFINE TABLE IF NOT EXISTS workflow_runs SCHEMAFULL
      CHANGEFEED 3d INCLUDE ORIGINAL;
    DEFINE FIELD IF NOT EXISTS runId ON TABLE workflow_runs TYPE string;
    DEFINE FIELD IF NOT EXISTS deploymentId ON TABLE workflow_runs TYPE string;
    DEFINE FIELD IF NOT EXISTS workflowName ON TABLE workflow_runs TYPE string;
    DEFINE FIELD IF NOT EXISTS status ON TABLE workflow_runs TYPE string;
    DEFINE FIELD IF NOT EXISTS specVersion ON TABLE workflow_runs TYPE option<int>;
    DEFINE FIELD IF NOT EXISTS executionContext ON TABLE workflow_runs TYPE option<object> FLEXIBLE;
    DEFINE FIELD IF NOT EXISTS input ON TABLE workflow_runs TYPE bytes;
    DEFINE FIELD IF NOT EXISTS output ON TABLE workflow_runs TYPE option<bytes>;
    DEFINE FIELD IF NOT EXISTS error ON TABLE workflow_runs TYPE option<object> FLEXIBLE;
    DEFINE FIELD IF NOT EXISTS createdAt ON TABLE workflow_runs TYPE datetime;
    DEFINE FIELD IF NOT EXISTS updatedAt ON TABLE workflow_runs TYPE datetime;
    DEFINE FIELD IF NOT EXISTS startedAt ON TABLE workflow_runs TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS completedAt ON TABLE workflow_runs TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS expiredAt ON TABLE workflow_runs TYPE option<datetime>;
    DEFINE INDEX IF NOT EXISTS workflow_runs_workflow_status_run ON TABLE workflow_runs
      FIELDS workflowName, status, runId;
  

    DEFINE TABLE IF NOT EXISTS workflow_events SCHEMAFULL
      CHANGEFEED 3d INCLUDE ORIGINAL;
    DEFINE FIELD IF NOT EXISTS runId ON TABLE workflow_events TYPE string;
    DEFINE FIELD IF NOT EXISTS eventId ON TABLE workflow_events TYPE string;
    DEFINE FIELD IF NOT EXISTS eventType ON TABLE workflow_events TYPE string;
    DEFINE FIELD IF NOT EXISTS correlationId ON TABLE workflow_events TYPE option<string>;
    DEFINE FIELD IF NOT EXISTS eventData ON TABLE workflow_events TYPE option<object> FLEXIBLE;
    DEFINE FIELD IF NOT EXISTS createdAt ON TABLE workflow_events TYPE datetime;
    DEFINE FIELD IF NOT EXISTS specVersion ON TABLE workflow_events TYPE option<int>;
    DEFINE INDEX IF NOT EXISTS workflow_events_run_event ON TABLE workflow_events
      FIELDS runId, eventId;
    DEFINE INDEX IF NOT EXISTS workflow_events_correlation_event ON TABLE workflow_events
      FIELDS correlationId, eventId;
  

    DEFINE TABLE IF NOT EXISTS workflow_steps SCHEMAFULL
      CHANGEFEED 3d INCLUDE ORIGINAL;
    DEFINE FIELD IF NOT EXISTS runId ON TABLE workflow_steps TYPE string;
    DEFINE FIELD IF NOT EXISTS stepId ON TABLE workflow_steps TYPE string;
    DEFINE FIELD IF NOT EXISTS stepName ON TABLE workflow_steps TYPE string;
    DEFINE FIELD IF NOT EXISTS status ON TABLE workflow_steps TYPE string;
    DEFINE FIELD IF NOT EXISTS input ON TABLE workflow_steps TYPE bytes;
    DEFINE FIELD IF NOT EXISTS output ON TABLE workflow_steps TYPE option<bytes>;
    DEFINE FIELD IF NOT EXISTS error ON TABLE workflow_steps TYPE option<object> FLEXIBLE;
    DEFINE FIELD IF NOT EXISTS attempt ON TABLE workflow_steps TYPE int;
    DEFINE FIELD IF NOT EXISTS startedAt ON TABLE workflow_steps TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS completedAt ON TABLE workflow_steps TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS createdAt ON TABLE workflow_steps TYPE datetime;
    DEFINE FIELD IF NOT EXISTS updatedAt ON TABLE workflow_steps TYPE datetime;
    DEFINE FIELD IF NOT EXISTS retryAfter ON TABLE workflow_steps TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS specVersion ON TABLE workflow_steps TYPE option<int>;
    DEFINE INDEX IF NOT EXISTS workflow_steps_run_step ON TABLE workflow_steps
      FIELDS runId, stepId;
    DEFINE INDEX IF NOT EXISTS workflow_steps_step_id ON TABLE workflow_steps
      FIELDS stepId;
  

    DEFINE TABLE IF NOT EXISTS workflow_hooks SCHEMAFULL
      CHANGEFEED 3d INCLUDE ORIGINAL;
    DEFINE FIELD IF NOT EXISTS runId ON TABLE workflow_hooks TYPE string;
    DEFINE FIELD IF NOT EXISTS hookId ON TABLE workflow_hooks TYPE string;
    DEFINE FIELD IF NOT EXISTS token ON TABLE workflow_hooks TYPE string;
    DEFINE FIELD OVERWRITE tokenHash ON TABLE workflow_hooks TYPE string
      VALUE crypto::sha256(token) READONLY;
    DEFINE FIELD IF NOT EXISTS metadata ON TABLE workflow_hooks TYPE option<bytes>;
    DEFINE FIELD IF NOT EXISTS ownerId ON TABLE workflow_hooks TYPE string;
    DEFINE FIELD IF NOT EXISTS projectId ON TABLE workflow_hooks TYPE string;
    DEFINE FIELD IF NOT EXISTS environment ON TABLE workflow_hooks TYPE string;
    DEFINE FIELD IF NOT EXISTS createdAt ON TABLE workflow_hooks TYPE datetime;
    DEFINE FIELD IF NOT EXISTS specVersion ON TABLE workflow_hooks TYPE option<int>;
    DEFINE FIELD IF NOT EXISTS isWebhook ON TABLE workflow_hooks TYPE bool;
    DEFINE INDEX IF NOT EXISTS workflow_hooks_run_hook ON TABLE workflow_hooks
      FIELDS runId, hookId;
    DEFINE INDEX IF NOT EXISTS workflow_hooks_token_hash ON TABLE workflow_hooks
      FIELDS tokenHash UNIQUE;
  

    DEFINE TABLE IF NOT EXISTS workflow_waits SCHEMAFULL
      CHANGEFEED 3d INCLUDE ORIGINAL;
    DEFINE FIELD IF NOT EXISTS waitId ON TABLE workflow_waits TYPE string;
    DEFINE FIELD IF NOT EXISTS runId ON TABLE workflow_waits TYPE string;
    DEFINE FIELD IF NOT EXISTS status ON TABLE workflow_waits TYPE string;
    DEFINE FIELD IF NOT EXISTS resumeAt ON TABLE workflow_waits TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS completedAt ON TABLE workflow_waits TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS createdAt ON TABLE workflow_waits TYPE datetime;
    DEFINE FIELD IF NOT EXISTS updatedAt ON TABLE workflow_waits TYPE datetime;
    DEFINE FIELD IF NOT EXISTS specVersion ON TABLE workflow_waits TYPE option<int>;
    DEFINE INDEX IF NOT EXISTS workflow_waits_run_wait ON TABLE workflow_waits
      FIELDS runId, waitId;
  

    DEFINE TABLE IF NOT EXISTS workflow_streams SCHEMAFULL
      CHANGEFEED 3d INCLUDE ORIGINAL;
    DEFINE FIELD IF NOT EXISTS streamId ON TABLE workflow_streams TYPE string;
    DEFINE FIELD IF NOT EXISTS runId ON TABLE workflow_streams TYPE string;
    DEFINE FIELD IF NOT EXISTS tailIndex ON TABLE workflow_streams TYPE int;
    DEFINE FIELD IF NOT EXISTS done ON TABLE workflow_streams TYPE bool;
    DEFINE FIELD IF NOT EXISTS createdAt ON TABLE workflow_streams TYPE datetime;
    DEFINE FIELD IF NOT EXISTS updatedAt ON TABLE workflow_streams TYPE datetime;
    DEFINE FIELD IF NOT EXISTS closedAt ON TABLE workflow_streams TYPE option<datetime>;
    DEFINE INDEX IF NOT EXISTS workflow_streams_run_stream ON TABLE workflow_streams
      FIELDS runId, streamId;
  

    DEFINE TABLE IF NOT EXISTS workflow_stream_chunks SCHEMAFULL;
    DEFINE FIELD IF NOT EXISTS streamId ON TABLE workflow_stream_chunks TYPE string;
    DEFINE FIELD IF NOT EXISTS runId ON TABLE workflow_stream_chunks TYPE string;
    DEFINE FIELD IF NOT EXISTS chunkId ON TABLE workflow_stream_chunks TYPE string;
    DEFINE FIELD IF NOT EXISTS index ON TABLE workflow_stream_chunks TYPE int;
    DEFINE FIELD IF NOT EXISTS data ON TABLE workflow_stream_chunks TYPE bytes;
    DEFINE FIELD IF NOT EXISTS eof ON TABLE workflow_stream_chunks TYPE bool;
    DEFINE FIELD IF NOT EXISTS createdAt ON TABLE workflow_stream_chunks TYPE datetime;
    DEFINE INDEX IF NOT EXISTS workflow_stream_chunks_stream_chunk ON TABLE workflow_stream_chunks
      FIELDS streamId, chunkId;
  

    DEFINE TABLE IF NOT EXISTS workflow_queue_jobs SCHEMAFULL;
    DEFINE FIELD IF NOT EXISTS messageId ON TABLE workflow_queue_jobs TYPE string;
    DEFINE FIELD IF NOT EXISTS version ON TABLE workflow_queue_jobs TYPE int;
    DEFINE FIELD IF NOT EXISTS laneKey ON TABLE workflow_queue_jobs TYPE string;
    DEFINE FIELD IF NOT EXISTS queueName ON TABLE workflow_queue_jobs TYPE string;
    DEFINE FIELD IF NOT EXISTS queuePrefix ON TABLE workflow_queue_jobs TYPE string;
    DEFINE FIELD IF NOT EXISTS queueId ON TABLE workflow_queue_jobs TYPE string;
    DEFINE FIELD IF NOT EXISTS workflowRunId ON TABLE workflow_queue_jobs TYPE option<string>;
    DEFINE FIELD IF NOT EXISTS body ON TABLE workflow_queue_jobs TYPE bytes;
    DEFINE FIELD IF NOT EXISTS headers ON TABLE workflow_queue_jobs TYPE option<object> FLEXIBLE;
    DEFINE FIELD IF NOT EXISTS idempotencyKey ON TABLE workflow_queue_jobs TYPE option<string>;
    DEFINE FIELD IF NOT EXISTS attempt ON TABLE workflow_queue_jobs TYPE int;
    DEFINE FIELD IF NOT EXISTS status ON TABLE workflow_queue_jobs TYPE string;
    DEFINE FIELD IF NOT EXISTS availableAt ON TABLE workflow_queue_jobs TYPE datetime;
    DEFINE FIELD IF NOT EXISTS leaseUntil ON TABLE workflow_queue_jobs TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS lockedBy ON TABLE workflow_queue_jobs TYPE option<string>;
    DEFINE FIELD IF NOT EXISTS createdAt ON TABLE workflow_queue_jobs TYPE datetime;
    DEFINE FIELD IF NOT EXISTS updatedAt ON TABLE workflow_queue_jobs TYPE datetime;
    DEFINE FIELD IF NOT EXISTS completedAt ON TABLE workflow_queue_jobs TYPE option<datetime>;
    DEFINE INDEX IF NOT EXISTS workflow_queue_jobs_message_id ON TABLE workflow_queue_jobs
      FIELDS messageId UNIQUE;
    DEFINE INDEX IF NOT EXISTS workflow_queue_jobs_status_available ON TABLE workflow_queue_jobs
      FIELDS status, availableAt, messageId;
    DEFINE INDEX IF NOT EXISTS workflow_queue_jobs_status_lease ON TABLE workflow_queue_jobs
      FIELDS status, leaseUntil, messageId;
    DEFINE INDEX IF NOT EXISTS workflow_queue_jobs_lane_status_available ON TABLE workflow_queue_jobs
      FIELDS laneKey, status, availableAt, messageId;
    DEFINE INDEX IF NOT EXISTS workflow_queue_jobs_idempotency ON TABLE workflow_queue_jobs
      FIELDS idempotencyKey;
    DEFINE INDEX IF NOT EXISTS workflow_queue_jobs_idempotency_status_created ON TABLE workflow_queue_jobs
      FIELDS idempotencyKey, status, createdAt, messageId;
  

    DEFINE TABLE IF NOT EXISTS workflow_queue_lanes SCHEMAFULL;
    DEFINE FIELD IF NOT EXISTS laneKey ON TABLE workflow_queue_lanes TYPE string;
    DEFINE FIELD IF NOT EXISTS queueName ON TABLE workflow_queue_lanes TYPE string;
    DEFINE FIELD IF NOT EXISTS queuePrefix ON TABLE workflow_queue_lanes TYPE string;
    DEFINE FIELD IF NOT EXISTS queueId ON TABLE workflow_queue_lanes TYPE string;
    DEFINE FIELD IF NOT EXISTS queuedCount ON TABLE workflow_queue_lanes TYPE int;
    DEFINE FIELD IF NOT EXISTS readyCount ON TABLE workflow_queue_lanes TYPE int;
    DEFINE FIELD IF NOT EXISTS processingCount ON TABLE workflow_queue_lanes TYPE int;
    DEFINE FIELD IF NOT EXISTS nextAvailableAt ON TABLE workflow_queue_lanes TYPE option<datetime>;
    DEFINE FIELD IF NOT EXISTS updatedAt ON TABLE workflow_queue_lanes TYPE datetime;
    DEFINE INDEX IF NOT EXISTS workflow_queue_lanes_next_available ON TABLE workflow_queue_lanes
      FIELDS nextAvailableAt, queueName;
    DEFINE INDEX IF NOT EXISTS workflow_queue_lanes_ready_count ON TABLE workflow_queue_lanes
      FIELDS readyCount, nextAvailableAt, queueName;
  

    DEFINE TABLE IF NOT EXISTS workflow_queue_lane_claims SCHEMAFULL;
    DEFINE FIELD IF NOT EXISTS laneKey ON TABLE workflow_queue_lane_claims TYPE string;
    DEFINE FIELD IF NOT EXISTS updatedAt ON TABLE workflow_queue_lane_claims TYPE datetime;
    DEFINE FIELD IF NOT EXISTS drainingBy ON TABLE workflow_queue_lane_claims TYPE option<string>;
    DEFINE FIELD IF NOT EXISTS drainingUntil ON TABLE workflow_queue_lane_claims TYPE option<datetime>;
  

    DEFINE TABLE IF NOT EXISTS workflow_queue_signals SCHEMAFULL;
    DEFINE FIELD IF NOT EXISTS signalId ON TABLE workflow_queue_signals TYPE string;
    DEFINE FIELD IF NOT EXISTS updatedAt ON TABLE workflow_queue_signals TYPE datetime;
    DEFINE FIELD IF NOT EXISTS drainingBy ON TABLE workflow_queue_signals TYPE option<string>;
    DEFINE FIELD IF NOT EXISTS drainingUntil ON TABLE workflow_queue_signals TYPE option<datetime>;
    UPSERT workflow_queue_signals:drain SET
      signalId = "drain",
      updatedAt = time::now(),
      drainingBy = NONE,
      drainingUntil = NONE;
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::enqueue_dedupe(
      $recordKey: string,
      $messageId: string,
      $laneKey: string,
      $queueName: string,
      $queuePrefix: string,
      $queueId: string,
      $workflowRunId: option<string>,
      $body: bytes,
      $headers: option<object>,
      $idempotencyKey: string,
      $delay: duration
    ) {
      LET $now = time::now();
      LET $existing = (
        SELECT messageId, createdAt FROM workflow_queue_jobs
        WHERE idempotencyKey == $idempotencyKey
          AND status != "completed"
        ORDER BY createdAt DESC, messageId DESC
        LIMIT 1
      )[0].messageId;

      RETURN IF $existing != NONE {
        $existing
      } ELSE {
        LET $created = (
          CREATE ONLY type::record("workflow_queue_jobs", $recordKey) CONTENT {
            messageId: $messageId,
            version: 1,
            laneKey: $laneKey,
            queueName: $queueName,
            queuePrefix: $queuePrefix,
            queueId: $queueId,
            workflowRunId: $workflowRunId,
            body: $body,
            headers: $headers,
            idempotencyKey: $idempotencyKey,
            attempt: 1,
            status: "queued",
            availableAt: $now + $delay,
            createdAt: $now,
            updatedAt: $now
          }
        );

        fn::workflow::queue::touch_lane_enqueue(
          $laneKey,
          $queueName,
          $queuePrefix,
          $queueId,
          $created.availableAt
        );

        RETURN $messageId;
      };
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::enqueue_simple(
      $recordKey: string,
      $messageId: string,
      $laneKey: string,
      $queueName: string,
      $queuePrefix: string,
      $queueId: string,
      $workflowRunId: option<string>,
      $body: bytes,
      $headers: option<object>,
      $delay: duration
    ) {
      LET $now = time::now();
      CREATE ONLY type::record("workflow_queue_jobs", $recordKey) CONTENT {
        messageId: $messageId,
        version: 1,
        laneKey: $laneKey,
        queueName: $queueName,
        queuePrefix: $queuePrefix,
        queueId: $queueId,
        workflowRunId: $workflowRunId,
        body: $body,
        headers: $headers,
        attempt: 1,
        status: "queued",
        availableAt: $now + $delay,
        createdAt: $now,
        updatedAt: $now
      };

      fn::workflow::queue::touch_lane_enqueue(
        $laneKey,
        $queueName,
        $queuePrefix,
        $queueId,
        $now + $delay
      );

      RETURN $messageId;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::ensure_lane_claim(
      $laneKey: string
    ) {
      LET $now = time::now();

      RETURN (
        UPSERT ONLY type::record("workflow_queue_lane_claims", $laneKey)
        SET
          laneKey = $laneKey,
          updatedAt = IF updatedAt = NONE { $now } ELSE { updatedAt }
        RETURN AFTER
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::clamp_count($value: int) -> int {
      RETURN IF $value < 0 { 0 } ELSE { $value };
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::lane_next_available_at(
      $laneKey: string
    ) -> option<datetime> {
      RETURN (
        SELECT availableAt, messageId FROM workflow_queue_jobs
        WHERE laneKey == $laneKey
          AND status == "queued"
        ORDER BY availableAt ASC, messageId ASC
        LIMIT 1
      )[0].availableAt;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::apply_lane_delta(
      $laneKey: string,
      $queuedDelta: int,
      $readyDelta: int,
      $processingDelta: int,
      $nextAvailableCandidate: option<datetime>,
      $recomputeNextAvailable: bool
    ) {
      LET $now = time::now();
      LET $recomputedNextAvailableAt = IF $recomputeNextAvailable {
        fn::workflow::queue::lane_next_available_at($laneKey)
      } ELSE {
        NONE
      };

      RETURN (
        UPDATE ONLY type::record("workflow_queue_lanes", $laneKey)
        SET
          queuedCount = fn::workflow::queue::clamp_count(
            (IF queuedCount = NONE { 0 } ELSE { queuedCount }) + $queuedDelta
          ),
          readyCount = fn::workflow::queue::clamp_count(
            (IF readyCount = NONE { 0 } ELSE { readyCount }) + $readyDelta
          ),
          processingCount = fn::workflow::queue::clamp_count(
            (IF processingCount = NONE { 0 } ELSE { processingCount }) + $processingDelta
          ),
          nextAvailableAt = IF fn::workflow::queue::clamp_count(
            (IF queuedCount = NONE { 0 } ELSE { queuedCount }) + $queuedDelta
          ) <= 0 {
            NONE
          } ELSE IF $recomputedNextAvailableAt != NONE {
            $recomputedNextAvailableAt
          } ELSE IF (
            $nextAvailableCandidate != NONE
            AND (
              nextAvailableAt = NONE
              OR nextAvailableAt > $nextAvailableCandidate
            )
          ) {
            $nextAvailableCandidate
          } ELSE {
            nextAvailableAt
          },
          updatedAt = $now
        RETURN AFTER
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::touch_lane_enqueue(
      $laneKey: string,
      $queueName: string,
      $queuePrefix: string,
      $queueId: string,
      $availableAt: datetime
    ) {
      LET $now = time::now();
      LET $isReady = $availableAt <= $now;

      LET $lane = (
        UPSERT ONLY type::record("workflow_queue_lanes", $laneKey)
        SET
          laneKey = $laneKey,
          queueName = $queueName,
          queuePrefix = $queuePrefix,
          queueId = $queueId,
          queuedCount = (IF queuedCount = NONE { 0 } ELSE { queuedCount }) + 1,
          readyCount = (IF readyCount = NONE { 0 } ELSE { readyCount }) + IF $isReady { 1 } ELSE { 0 },
          processingCount = IF processingCount = NONE { 0 } ELSE { processingCount },
          nextAvailableAt = IF nextAvailableAt = NONE OR nextAvailableAt > $availableAt {
            $availableAt
          } ELSE {
            nextAvailableAt
          },
          updatedAt = $now
        RETURN AFTER
      );
      RETURN $lane;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::refresh_lane_ready_state(
      $laneKey: string,
      $queueName: string,
      $queuePrefix: string,
      $queueId: string
    ) {
      LET $now = time::now();
      LET $queuedCount = count((
        SELECT VALUE messageId FROM workflow_queue_jobs
        WHERE laneKey == $laneKey
          AND status == "queued"
      ));
      LET $readyCount = count((
        SELECT VALUE messageId FROM workflow_queue_jobs
        WHERE laneKey == $laneKey
          AND status == "queued"
          AND availableAt <= $now
      ));

      RETURN (
        UPSERT ONLY type::record("workflow_queue_lanes", $laneKey)
        SET
          laneKey = $laneKey,
          queueName = $queueName,
          queuePrefix = $queuePrefix,
          queueId = $queueId,
          queuedCount = $queuedCount,
          readyCount = $readyCount,
          processingCount = IF processingCount = NONE {
            0
          } ELSE {
            processingCount
          },
          nextAvailableAt = fn::workflow::queue::lane_next_available_at($laneKey),
          updatedAt = $now
        RETURN AFTER
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::retry_delay($attempt: int) -> duration {
      RETURN 1s * math::ceil(
        math::pow(math::e, math::clamp($attempt, 1, 10))
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::requeue_claimed_only(
      $job: record<workflow_queue_jobs>,
      $workerId: string,
      $delay: duration,
      $incrementAttempt: bool,
      $now: datetime
    ) {
      LET $attemptDelta = IF $incrementAttempt { 1 } ELSE { 0 };
      RETURN (
        UPDATE ONLY $job
        SET
          version = version + 1,
          status = "queued",
          lockedBy = NONE,
          leaseUntil = NONE,
          attempt = attempt + $attemptDelta,
          availableAt = $now + $delay,
          updatedAt = $now
        WHERE status == "processing"
          AND lockedBy == $workerId
        RETURN AFTER
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::requeue_claimed(
      $job: record<workflow_queue_jobs>,
      $workerId: string,
      $delay: duration,
      $incrementAttempt: bool
    ) {
      LET $now = time::now();
      LET $updated = fn::workflow::queue::requeue_claimed_only(
        $job,
        $workerId,
        $delay,
        $incrementAttempt,
        $now
      );

      IF $updated != NONE {
        fn::workflow::queue::apply_lane_delta(
          $updated.laneKey,
          1,
          IF $updated.availableAt <= $now { 1 } ELSE { 0 },
          -1,
          $updated.availableAt,
          false
        );
      };

      RETURN $updated;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::retry_claimed(
      $job: record<workflow_queue_jobs>,
      $workerId: string,
      $attempt: int
    ) {
      RETURN fn::workflow::queue::requeue_claimed(
        $job,
        $workerId,
        fn::workflow::queue::retry_delay($attempt),
        true
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::claim_candidate(
      $job: record<workflow_queue_jobs>,
      $workerId: string,
      $lease: duration,
      $now: datetime
    ) {
      RETURN (
        UPDATE ONLY $job
        SET
          version = version + 1,
          status = "processing",
          lockedBy = $workerId,
          leaseUntil = $now + $lease,
          updatedAt = $now
        WHERE status == "queued"
          AND availableAt <= $now
        RETURN AFTER
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::complete_claimed_only(
      $job: record<workflow_queue_jobs>,
      $workerId: string
    ) {
      RETURN (
        DELETE ONLY $job
        WHERE status == "processing"
          AND lockedBy == $workerId
        RETURN BEFORE
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::complete_claimed(
      $job: record<workflow_queue_jobs>,
      $workerId: string
    ) {
      LET $deleted = fn::workflow::queue::complete_claimed_only($job, $workerId);

      IF $deleted != NONE {
        fn::workflow::queue::apply_lane_delta(
          $deleted.laneKey,
          0,
          0,
          -1,
          NONE,
          false
        );
      };

      RETURN $deleted;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::complete_many(
      $workerId: string,
      $jobs: array<record<workflow_queue_jobs>>
    ) {
      IF count($jobs) <= 0 {
        RETURN [];
      };

      LET $completed = SELECT VALUE
        fn::workflow::queue::complete_claimed_only($this, $workerId)
      FROM $jobs;
      LET $rows = $completed.filter(|$job| $job != NONE);
      LET $laneDeltas = SELECT
        laneKey,
        count() AS jobCount
      FROM $rows
      GROUP BY laneKey;
      LET $_ = SELECT VALUE
        fn::workflow::queue::apply_lane_delta(
          $this.laneKey,
          0,
          0,
          -$this.jobCount,
          NONE,
          false
      )
      FROM $laneDeltas;

      RETURN SELECT VALUE messageId FROM $rows;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::requeue_many(
      $workerId: string,
      $jobs: array<record<workflow_queue_jobs>>,
      $delay: duration,
      $incrementAttempt: bool
    ) {
      IF count($jobs) <= 0 {
        RETURN [];
      };

      LET $now = time::now();
      LET $nextAvailableAt = $now + $delay;
      LET $readyPerJob = IF $nextAvailableAt <= $now { 1 } ELSE { 0 };
      LET $requeued = SELECT VALUE
        fn::workflow::queue::requeue_claimed_only(
          $this,
          $workerId,
          $delay,
          $incrementAttempt,
          $now
        )
      FROM $jobs;
      LET $rows = $requeued.filter(|$job| $job != NONE);
      LET $laneDeltas = SELECT
        laneKey,
        count() AS jobCount
      FROM $rows
      GROUP BY laneKey;
      LET $_ = SELECT VALUE
        fn::workflow::queue::apply_lane_delta(
          $this.laneKey,
          $this.jobCount,
          $this.jobCount * $readyPerJob,
          -$this.jobCount,
          $nextAvailableAt,
          false
      )
      FROM $laneDeltas;

      RETURN SELECT VALUE messageId FROM $rows;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::retry_many(
      $workerId: string,
      $jobs: array<record<workflow_queue_jobs>>,
      $attempt: int
    ) {
      IF count($jobs) <= 0 {
        RETURN [];
      };

      LET $now = time::now();
      LET $delay = fn::workflow::queue::retry_delay($attempt);
      LET $nextAvailableAt = $now + $delay;
      LET $readyPerJob = IF $nextAvailableAt <= $now { 1 } ELSE { 0 };
      LET $retried = SELECT VALUE
        fn::workflow::queue::requeue_claimed_only(
          $this,
          $workerId,
          $delay,
          true,
          $now
        )
      FROM $jobs;
      LET $rows = $retried.filter(|$job| $job != NONE);
      LET $laneDeltas = SELECT
        laneKey,
        count() AS jobCount
      FROM $rows
      GROUP BY laneKey;
      LET $_ = SELECT VALUE
        fn::workflow::queue::apply_lane_delta(
          $this.laneKey,
          $this.jobCount,
          $this.jobCount * $readyPerJob,
          -$this.jobCount,
          $nextAvailableAt,
          false
      )
      FROM $laneDeltas;

      RETURN SELECT VALUE messageId FROM $rows;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::claim_lane_candidate(
      $laneKey: string,
      $workerId: string,
      $lease: duration,
      $now: datetime
    ) {
      fn::workflow::queue::ensure_lane_claim($laneKey);

      LET $claimed = (
        UPDATE ONLY type::record("workflow_queue_lane_claims", $laneKey)
        SET
          updatedAt = $now,
          drainingBy = $workerId,
          drainingUntil = $now + $lease
        WHERE (
            drainingBy = NONE
            OR drainingUntil = NONE
            OR drainingUntil <= $now
            OR drainingBy == $workerId
          )
        RETURN AFTER
      );

      RETURN IF $claimed != NONE { $laneKey } ELSE { NONE };
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::recover_candidate(
      $job: record<workflow_queue_jobs>,
      $now: datetime
    ) {
      RETURN (
        UPDATE ONLY $job
        SET
          version = version + 1,
          status = "queued",
          lockedBy = NONE,
          leaseUntil = NONE,
          availableAt = $now,
          updatedAt = $now
        WHERE status == "processing"
          AND leaseUntil != NONE
          AND leaseUntil <= $now
        RETURN AFTER
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::claim_ready_lanes(
      $workerId: string,
      $lease: duration,
      $limit: int
    ) {
      IF $limit <= 0 {
        RETURN [];
      };

      LET $now = time::now();
      LET $candidates = (
        SELECT laneKey, nextAvailableAt, queueName FROM workflow_queue_lanes
        WHERE readyCount > 0
        ORDER BY nextAvailableAt ASC, queueName ASC
        LIMIT $limit
      );
      LET $claimed = SELECT VALUE
        fn::workflow::queue::claim_lane_candidate($this.laneKey, $workerId, $lease, $now)
      FROM $candidates;

      RETURN $claimed.filter(|$lane| $lane != NONE);
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::release_lane_claim(
      $laneKey: string,
      $workerId: string
    ) {
      LET $now = time::now();

      RETURN (
        UPDATE ONLY type::record("workflow_queue_lane_claims", $laneKey)
        SET
          updatedAt = $now,
          drainingBy = NONE,
          drainingUntil = NONE
        WHERE drainingBy == $workerId
        RETURN AFTER
      );
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::refresh_due_lanes($limit: int) {
      IF $limit <= 0 {
        RETURN 0;
      };

      LET $now = time::now();
      LET $candidates = (
        SELECT laneKey, queueName, queuePrefix, queueId, nextAvailableAt FROM workflow_queue_lanes
        WHERE queuedCount > 0
          AND readyCount <= 0
          AND nextAvailableAt != NONE
          AND nextAvailableAt <= $now
        ORDER BY nextAvailableAt ASC, queueName ASC
        LIMIT $limit
      );
      LET $refreshed = SELECT VALUE
        fn::workflow::queue::refresh_lane_ready_state(
          $this.laneKey,
          $this.queueName,
          $this.queuePrefix,
          $this.queueId
        )
      FROM $candidates;

      RETURN count($refreshed.filter(|$lane| $lane != NONE AND $lane.readyCount > 0));
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::claim_many_in_lane(
      $laneKey: string,
      $workerId: string,
      $lease: duration,
      $limit: int
    ) {
      IF $limit <= 0 {
        RETURN [];
      };

      LET $now = time::now();
      LET $candidates = (
        SELECT id, availableAt, messageId FROM workflow_queue_jobs
        WHERE laneKey == $laneKey
          AND status == "queued"
          AND availableAt <= $now
        ORDER BY availableAt ASC, messageId ASC
        LIMIT $limit
      ).id;
      LET $claimed = SELECT VALUE
        fn::workflow::queue::claim_candidate($this, $workerId, $lease, $now)
      FROM $candidates;
      LET $jobs = $claimed.filter(|$job| $job != NONE);

      IF count($jobs) > 0 {
        fn::workflow::queue::apply_lane_delta(
          $laneKey,
          -count($jobs),
          -count($jobs),
          count($jobs),
          NONE,
          true
        );
      };

      RETURN SELECT VALUE {
        messageId: $this.messageId,
        queueName: $this.queueName,
        queuePrefix: $this.queuePrefix,
        workflowRunId: $this.workflowRunId,
        body: $this.body,
        headers: $this.headers,
        attempt: $this.attempt
      } FROM $jobs;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::claim_many_in_lane_and_release(
      $laneKey: string,
      $workerId: string,
      $lease: duration,
      $limit: int
    ) {
      LET $jobs = fn::workflow::queue::claim_many_in_lane(
        $laneKey,
        $workerId,
        $lease,
        $limit
      );

      fn::workflow::queue::release_lane_claim($laneKey, $workerId);

      RETURN $jobs;
    };
  

    DEFINE FUNCTION OVERWRITE fn::workflow::queue::recover_many($limit: int) {
      IF $limit <= 0 {
        RETURN 0;
      };

      LET $now = time::now();
      LET $candidates = (
        SELECT id, leaseUntil, messageId FROM workflow_queue_jobs
        WHERE status == "processing"
          AND leaseUntil != NONE
          AND leaseUntil <= $now
        ORDER BY leaseUntil ASC, messageId ASC
        LIMIT $limit
      ).id;
      LET $recovered = SELECT VALUE
        fn::workflow::queue::recover_candidate($this, $now)
      FROM $candidates;
      LET $jobs = $recovered.filter(|$job| $job != NONE);
      LET $_ = SELECT VALUE
        fn::workflow::queue::apply_lane_delta(
          $this.laneKey,
          1,
          1,
          -1,
          $this.availableAt,
          false
        )
      FROM $jobs;

      RETURN count($jobs);
    };
  
`;

export function createSchemaEnsurer(client: SurrealRpcClient) {
	let promise: Promise<void> | null = null;

	return async function ensureSchema(): Promise<void> {
		if (!promise) {
			promise = retrySurrealWrite(
				async () => {
					await client.query(schemaStatements);
				},
				{
					attempts: 12,
					minDelayMs: 20,
					maxDelayMs: 250,
				}
			);
		}

		await promise;
	};
}
