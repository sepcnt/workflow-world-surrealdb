import { JsonTransport } from '@vercel/queue';
import {
	MessageId,
	type Queue,
	QueuePayloadSchema,
	type QueuePrefix,
	type ValidQueueName,
	WorkflowInvokePayloadSchema,
} from '@workflow/world';
import { createLocalWorld } from '@workflow/world-local';
import { Duration, gt, RecordId } from 'surrealdb';
import { monotonicFactory } from 'ulid';
import {
	type ResolvedSurrealWorldConfig,
	resolveExecutionBaseUrl,
} from './config.js';
import { Tables } from './constants.js';
import type {
	ClaimedQueueJobRow,
	QueueJobRow,
	QueueLaneRow,
} from './records.js';
import {
	type LiveNotification,
	type LiveSubscription,
	SurrealRpcClient,
} from './rpc.js';
import type { EnsureSchema } from './storage-helpers.js';
import {
	coerceErrorMessage,
	isRetryableSurrealWriteConflict,
	retrySurrealWrite,
} from './util.js';

class Semaphore {
	private active = 0;
	private queue: Array<() => void> = [];

	constructor(private readonly capacity: number) {}

	available(): number {
		return Math.max(0, this.capacity - this.active);
	}

	async acquire(): Promise<() => void> {
		if (this.active < this.capacity) {
			this.active++;
			return () => this.release();
		}

		await new Promise<void>((resolve) => {
			this.queue.push(() => {
				this.active++;
				resolve();
			});
		});

		return () => this.release();
	}

	private release() {
		this.active--;
		const next = this.queue.shift();
		next?.();
	}
}

export type SurrealQueue = Queue & {
	start(): Promise<void>;
	close(): Promise<void>;
};

type QueueTerminalMutation =
	| {
			kind: 'complete';
			messageId: string;
			thing: RecordId;
			resolve: (value: boolean) => void;
			reject: (reason?: unknown) => void;
	  }
	| {
			kind: 'requeue';
			messageId: string;
			thing: RecordId;
			delayMs: number;
			incrementAttempt: boolean;
			resolve: (value: boolean) => void;
			reject: (reason?: unknown) => void;
	  }
	| {
			kind: 'retry';
			messageId: string;
			thing: RecordId;
			attempt: number;
			resolve: (value: boolean) => void;
			reject: (reason?: unknown) => void;
	  };

type QueueTerminalMutationInput =
	| {
			kind: 'complete';
			messageId: string;
			thing: RecordId;
	  }
	| {
			kind: 'requeue';
			messageId: string;
			thing: RecordId;
			delayMs: number;
			incrementAttempt: boolean;
	  }
	| {
			kind: 'retry';
			messageId: string;
			thing: RecordId;
			attempt: number;
	  };

const DEFAULT_QUEUE_DRAIN_LEASE_MS = 1_000;
const DEFAULT_QUEUE_IDLE_POLL_MS = 250;
const DEFAULT_QUEUE_TERMINAL_BATCH_SIZE = 32;
const QUEUE_MUTATION_RETRY_OPTIONS = {
	attempts: 12,
	minDelayMs: 10,
	maxDelayMs: 250,
} as const;

export function createQueue(
	config: ResolvedSurrealWorldConfig,
	client: SurrealRpcClient,
	ensureSchema: EnsureSchema
): SurrealQueue {
	const liveClient = new SurrealRpcClient(config);
	const transport = new JsonTransport();
	const generateMessageId = monotonicFactory();
	const localWorld = createLocalWorld();
	const semaphore = new Semaphore(config.queueConcurrency);
	const queueLeaseMs = config.queueLeaseMs;
	const queueHeartbeatMs = config.queueHeartbeatMs;
	const queueDrainLeaseMs = Math.max(
		DEFAULT_QUEUE_DRAIN_LEASE_MS,
		Math.min(queueLeaseMs, queueHeartbeatMs * 2)
	);
	const stepQueueLaneShards = config.stepQueueLaneShards;
	const workflowQueueLaneShards = config.workflowQueueLaneShards;
	const liveReconnectDelayMs = config.liveReconnectDelayMs;
	const workerId = `surreal-worker-${process.pid}-${generateMessageId()}`;
	const inflight = new Map<string, Promise<void>>();
	const inflightWorkflowRuns = new Map<string, Promise<void>>();
	const terminalMutations: QueueTerminalMutation[] = [];
	let executionBaseUrlPromise: Promise<string> | null = null;

	let startPromise: Promise<void> | null = null;
	let drainPromise: Promise<void> | null = null;
	let drainRequested = false;
	let closed = false;
	let wakeTimer: ReturnType<typeof globalThis.setTimeout> | null = null;
	let nextWakeAtMs: number | null = null;
	let terminalFlushTimer: ReturnType<typeof globalThis.setTimeout> | null =
		null;
	let terminalFlushPromise: Promise<void> | null = null;
	let liveFeed: LiveSubscription | null = null;
	let unsubscribeClose: (() => void) | null = null;

	function parseQueueName(name: ValidQueueName): [QueuePrefix, string] {
		const prefixes: QueuePrefix[] = ['__wkf_step_', '__wkf_workflow_'];
		for (const prefix of prefixes) {
			if (name.startsWith(prefix)) {
				return [prefix, name.slice(prefix.length)];
			}
		}

		throw new Error(`Invalid queue name: ${name}`);
	}

	function durationLiteralFromMs(ms: number): string {
		return `${Math.max(0, Math.ceil(ms))}ms`;
	}

	function toBinaryPayload(
		value: string | Uint8Array | ArrayBuffer
	): Uint8Array | ArrayBuffer {
		return typeof value === 'string'
			? Uint8Array.from(Buffer.from(value))
			: value;
	}

	function durationValueFromMs(ms: number): Duration {
		return new Duration(durationLiteralFromMs(ms));
	}

	function computeRetryDelayMs(attempt: number): number {
		return Math.ceil(Math.exp(Math.max(1, Math.min(attempt, 10)))) * 1000;
	}

	function getExecutionBaseUrl(): Promise<string> {
		executionBaseUrlPromise ??= resolveExecutionBaseUrl(config);
		return executionBaseUrlPromise;
	}

	function hashLaneValue(value: string): number {
		let hash = 0;
		for (let index = 0; index < value.length; index += 1) {
			hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
		}

		return hash;
	}

	function buildLaneKey(args: {
		messageId: string;
		queueName: ValidQueueName;
		queuePrefix: QueuePrefix;
		workflowRunId?: string;
	}): string {
		const laneShard =
			hashLaneValue(args.workflowRunId || args.messageId) %
			(args.queuePrefix === '__wkf_workflow_'
				? workflowQueueLaneShards
				: stepQueueLaneShards);
		return `${args.queueName}:shard:${laneShard}`;
	}

	function getQueueRoute(queueName: ValidQueueName): 'flow' | 'step' {
		if (queueName.startsWith('__wkf_step_')) {
			return 'step';
		}

		if (queueName.startsWith('__wkf_workflow_')) {
			return 'flow';
		}

		throw new Error('Unknown queue name prefix');
	}

	async function queryValue<T>(
		sql: string,
		vars?: Record<string, unknown>
	): Promise<T> {
		const results = await client.query<T>(sql, vars);
		return results.at(-1) as T;
	}

	async function callQueueFunction<T>(
		name: string,
		args: unknown[] = []
	): Promise<T> {
		return client.run<T>(name, args);
	}

	async function retryQueueMutation<T>(
		operation: () => Promise<T>
	): Promise<T> {
		return retrySurrealWrite(operation, QUEUE_MUTATION_RETRY_OPTIONS);
	}

	function normalizeRows<T>(rows: T | T[] | null | undefined): T[] {
		if (!rows) {
			return [];
		}

		return Array.isArray(rows) ? rows.filter(Boolean) : [rows];
	}

	async function claimReadyLanes(limit: number): Promise<string[]> {
		if (limit <= 0) {
			return [];
		}

		const claimed = await retryQueueMutation(async () =>
			callQueueFunction<string | string[] | null>(
				'fn::workflow::queue::claim_ready_lanes',
				[workerId, durationValueFromMs(queueDrainLeaseMs), limit]
			)
		);

		return normalizeRows(claimed);
	}

	async function releaseLaneLease(laneKey: string): Promise<void> {
		try {
			await callQueueFunction('fn::workflow::queue::release_lane_claim', [
				laneKey,
				workerId,
			]);
		} catch (error) {
			if (closed || isRetryableQueueControlError(error)) {
				return;
			}

			throw error;
		}
	}

	async function claimManyReadyJobsInLaneAndRelease(
		laneKey: string,
		limit: number
	): Promise<ClaimedQueueJobRow[]> {
		if (limit <= 0) {
			return [];
		}

		const claimed = await retryQueueMutation(async () =>
			callQueueFunction<ClaimedQueueJobRow | ClaimedQueueJobRow[] | null>(
				'fn::workflow::queue::claim_many_in_lane_and_release',
				[laneKey, workerId, durationValueFromMs(queueLeaseMs), limit]
			)
		);

		return normalizeRows(claimed);
	}

	async function refreshDueLanes(limit: number): Promise<number> {
		if (limit <= 0) {
			return 0;
		}

		return retryQueueMutation(async () =>
			callQueueFunction<number>('fn::workflow::queue::refresh_due_lanes', [
				limit,
			])
		);
	}

	async function recoverManyExpiredJobs(limit: number): Promise<number> {
		if (limit <= 0) {
			return 0;
		}

		return retryQueueMutation(async () =>
			callQueueFunction<number>('fn::workflow::queue::recover_many', [limit])
		);
	}

	async function heartbeatJob(messageId: string): Promise<boolean> {
		const refreshed = await retryQueueMutation(async () =>
			queryValue<QueueJobRow | null>(
				`
          LET $now = time::now();
          RETURN (
            UPDATE ONLY $thing
            SET
              version = version + 1,
              leaseUntil = $now + <duration>$lease,
              updatedAt = $now
            WHERE status == "processing"
              AND lockedBy == $workerId
            RETURN AFTER
          );
        `,
				{
					thing: new RecordId(Tables.queueJobs, messageId),
					workerId,
					lease: durationLiteralFromMs(queueLeaseMs),
				}
			)
		);

		return Boolean(refreshed);
	}

	async function completeJob(messageId: string): Promise<boolean> {
		return enqueueTerminalMutation({
			kind: 'complete',
			messageId,
			thing: new RecordId(Tables.queueJobs, messageId),
		});
	}

	async function requeueJob(
		messageId: string,
		delayMs: number,
		incrementAttempt: boolean
	): Promise<boolean> {
		return enqueueTerminalMutation({
			kind: 'requeue',
			messageId,
			thing: new RecordId(Tables.queueJobs, messageId),
			delayMs,
			incrementAttempt,
		});
	}

	async function retryJob(
		messageId: string,
		attempt: number
	): Promise<boolean> {
		return enqueueTerminalMutation({
			kind: 'retry',
			messageId,
			thing: new RecordId(Tables.queueJobs, messageId),
			attempt,
		});
	}

	async function flushTerminalMutations(): Promise<void> {
		if (terminalFlushPromise) {
			await terminalFlushPromise;
			return;
		}

		clearTerminalFlushTimer();
		if (terminalMutations.length <= 0) {
			return;
		}

		const batch = terminalMutations.splice(0, terminalMutations.length);
		terminalFlushPromise = (async () => {
			const completeMutations = batch.filter(
				(
					mutation
				): mutation is Extract<QueueTerminalMutation, { kind: 'complete' }> =>
					mutation.kind === 'complete'
			);
			const requeueGroups = new Map<
				string,
				Array<Extract<QueueTerminalMutation, { kind: 'requeue' }>>
			>();
			const retryGroups = new Map<
				number,
				Array<Extract<QueueTerminalMutation, { kind: 'retry' }>>
			>();

			for (const mutation of batch) {
				if (mutation.kind === 'requeue') {
					const key = `${mutation.delayMs}:${mutation.incrementAttempt ? 1 : 0}`;
					const group = requeueGroups.get(key) || [];
					group.push(mutation);
					requeueGroups.set(key, group);
					continue;
				}

				if (mutation.kind === 'retry') {
					const group = retryGroups.get(mutation.attempt) || [];
					group.push(mutation);
					retryGroups.set(mutation.attempt, group);
				}
			}

			if (completeMutations.length > 0) {
				const completed = await retryQueueMutation(async () =>
					callQueueFunction<string | string[] | null>(
						'fn::workflow::queue::complete_many',
						[workerId, completeMutations.map((mutation) => mutation.thing)]
					)
				);
				const completedIds = new Set(normalizeRows(completed));
				for (const mutation of completeMutations) {
					mutation.resolve(completedIds.has(mutation.messageId));
				}
			}

			for (const mutations of requeueGroups.values()) {
				const template = mutations[0]!;
				const updated = await retryQueueMutation(async () =>
					callQueueFunction<string | string[] | null>(
						'fn::workflow::queue::requeue_many',
						[
							workerId,
							mutations.map((mutation) => mutation.thing),
							durationValueFromMs(template.delayMs),
							template.incrementAttempt,
						]
					)
				);
				const updatedIds = new Set(normalizeRows(updated));
				for (const mutation of mutations) {
					mutation.resolve(updatedIds.has(mutation.messageId));
				}
			}

			for (const [attempt, mutations] of retryGroups.entries()) {
				const updated = await retryQueueMutation(async () =>
					callQueueFunction<string | string[] | null>(
						'fn::workflow::queue::retry_many',
						[workerId, mutations.map((mutation) => mutation.thing), attempt]
					)
				);
				const updatedIds = new Set(normalizeRows(updated));
				for (const mutation of mutations) {
					mutation.resolve(updatedIds.has(mutation.messageId));
				}
			}
		})()
			.catch((error) => {
				for (const mutation of batch) {
					mutation.reject(error);
				}
			})
			.finally(() => {
				terminalFlushPromise = null;
				if (terminalMutations.length > 0) {
					void flushTerminalMutations().catch((error) => {
						reportBackgroundError('failed to flush terminal mutations', error);
					});
				}
			});

		await terminalFlushPromise;
	}

	function clearTerminalFlushTimer() {
		if (terminalFlushTimer) {
			globalThis.clearTimeout(terminalFlushTimer);
			terminalFlushTimer = null;
		}
	}

	function scheduleTerminalFlush() {
		if (terminalFlushTimer || terminalFlushPromise) {
			return;
		}

		terminalFlushTimer = globalThis.setTimeout(() => {
			terminalFlushTimer = null;
			void flushTerminalMutations().catch((error) => {
				reportBackgroundError('failed to flush terminal mutations', error);
			});
		}, 0);
	}

	function enqueueTerminalMutation(
		mutation: QueueTerminalMutationInput
	): Promise<boolean> {
		return new Promise<boolean>((resolve, reject) => {
			terminalMutations.push({
				...mutation,
				resolve,
				reject,
			} as QueueTerminalMutation);

			if (terminalMutations.length >= DEFAULT_QUEUE_TERMINAL_BATCH_SIZE) {
				void flushTerminalMutations().catch((error) => {
					reportBackgroundError('failed to flush terminal mutations', error);
				});
				return;
			}

			scheduleTerminalFlush();
		});
	}

	function clearWakeTimer() {
		if (wakeTimer) {
			globalThis.clearTimeout(wakeTimer);
			wakeTimer = null;
		}
	}

	function armWakeTimer(targetAtMs: number) {
		clearWakeTimer();
		nextWakeAtMs = targetAtMs;
		wakeTimer = globalThis.setTimeout(
			() => {
				wakeTimer = null;
				nextWakeAtMs = null;
				requestDrain();
			},
			Math.max(0, targetAtMs - Date.now())
		);
	}

	function noteDelayedWork(delayMs: number) {
		if (delayMs <= 0) {
			return;
		}

		const candidate = Date.now() + Math.max(0, Math.ceil(delayMs));
		if (nextWakeAtMs === null || candidate < nextWakeAtMs) {
			nextWakeAtMs = candidate;
			if (wakeTimer) {
				armWakeTimer(candidate);
			}
		}
	}

	function getNextWakeDelayMs(): number {
		if (nextWakeAtMs === null) {
			return DEFAULT_QUEUE_IDLE_POLL_MS;
		}

		const remaining = nextWakeAtMs - Date.now();
		if (remaining <= 0) {
			nextWakeAtMs = null;
			return DEFAULT_QUEUE_IDLE_POLL_MS;
		}

		return remaining;
	}

	function scheduleIdlePoll(delayMs = DEFAULT_QUEUE_IDLE_POLL_MS) {
		if (closed) {
			return;
		}

		const targetAtMs = Date.now() + Math.max(0, Math.ceil(delayMs));
		if (wakeTimer && nextWakeAtMs !== null && nextWakeAtMs <= targetAtMs) {
			return;
		}

		armWakeTimer(targetAtMs);
	}

	function reportBackgroundError(context: string, error: unknown) {
		if (closed && isRetryableQueueControlError(error)) {
			return;
		}

		console.error(
			`[world-surreal] ${context}: ${coerceErrorMessage(error)}`,
			error
		);
	}

	function isRetryableQueueControlError(error: unknown): boolean {
		return (
			isRetryableSurrealWriteConflict(error) ||
			error instanceof TypeError ||
			/connection closed|socket is not open|websocket/i.test(
				coerceErrorMessage(error)
			)
		);
	}

	function shouldSuppressQueueExecutionError(text: string): boolean {
		return (
			config.suppressBenchmarkErrors &&
			text.includes('Benchmark induced failure')
		);
	}

	function requestDrain() {
		if (closed) {
			return;
		}

		clearWakeTimer();
		drainRequested = true;
		if (drainPromise) {
			return;
		}

		drainPromise = (async () => {
			while (!closed && drainRequested) {
				drainRequested = false;
				await drainReadyJobs();
			}
		})()
			.catch((error) => {
				if (!closed) {
					scheduleIdlePoll(50);
				}
				reportBackgroundError('failed to drain queue jobs', error);
			})
			.finally(() => {
				drainPromise = null;
				if (drainRequested && !closed) {
					requestDrain();
				}
			});
	}

	async function drainReadyJobs(): Promise<void> {
		if (closed) {
			return;
		}

		if (semaphore.available() <= 0) {
			return;
		}

		while (!closed && semaphore.available() > 0) {
			const capacity = semaphore.available();
			const laneKeys = await claimReadyLanes(Math.max(capacity * 2, 1));
			let claimedAny = false;

			if (laneKeys.length > 0) {
				for (const laneKey of laneKeys) {
					if (closed || semaphore.available() <= 0) {
						break;
					}

					let claimed: ClaimedQueueJobRow[] = [];
					try {
						claimed = await claimManyReadyJobsInLaneAndRelease(
							laneKey,
							semaphore.available()
						);
					} catch (error) {
						await releaseLaneLease(laneKey).catch(() => {});
						if (!isRetryableSurrealWriteConflict(error)) {
							throw error;
						}
					}

					if (claimed.length === 0) {
						continue;
					}

					claimedAny = true;
					for (const row of claimed) {
						await dispatchClaimedJob(row);
					}
				}
			}

			if (claimedAny) {
				continue;
			}

			const recoveredCount = await recoverManyExpiredJobs(capacity);
			if (recoveredCount > 0) {
				continue;
			}

			const refreshedLaneCount = await refreshDueLanes(
				Math.max(capacity * 2, 1)
			);
			if (refreshedLaneCount > 0) {
				continue;
			}

			break;
		}

		if (!closed && semaphore.available() > 0 && !drainRequested) {
			scheduleIdlePoll(getNextWakeDelayMs());
		}
	}

	async function executeMessageOverHttp(
		row: ClaimedQueueJobRow
	): Promise<
		| { type: 'completed' }
		| { type: 'reschedule'; timeoutSeconds: number }
		| { type: 'error'; status: number; text: string }
	> {
		const headers: Record<string, string> = {
			...(row.headers || {}),
			'content-type': 'application/json',
			'x-vqs-queue-name': row.queueName,
			'x-vqs-message-id': row.messageId,
			'x-vqs-message-attempt': String(row.attempt),
		};
		const pathname = getQueueRoute(row.queueName as ValidQueueName);
		const body = toBinaryPayload(row.body);

		const response = await fetch(
			`${await getExecutionBaseUrl()}/.well-known/workflow/v1/${pathname}`,
			{
				method: 'POST',
				duplex: 'half',
				headers,
				body,
			} as any
		);
		const text = await response.text();

		if (!response.ok) {
			return { type: 'error', status: response.status, text };
		}

		try {
			const timeoutSeconds = Number(JSON.parse(text).timeoutSeconds);
			if (Number.isFinite(timeoutSeconds) && timeoutSeconds >= 0) {
				return { type: 'reschedule', timeoutSeconds };
			}
		} catch {}

		return { type: 'completed' };
	}

	async function processClaimedJob(row: ClaimedQueueJobRow): Promise<void> {
		const heartbeatMs = queueHeartbeatMs;
		const activeRow = row;
		let heartbeatStopped = false;
		let heartbeatPromise: Promise<void> = Promise.resolve();

		const stopHeartbeat = async () => {
			heartbeatStopped = true;
			globalThis.clearInterval(heartbeat);
			await heartbeatPromise.catch(() => {});
		};

		const heartbeat = globalThis.setInterval(() => {
			if (heartbeatStopped) {
				return;
			}

			heartbeatPromise = heartbeatPromise
				.catch(() => {})
				.then(async () => {
					if (heartbeatStopped) {
						return;
					}

					const refreshed = await heartbeatJob(activeRow.messageId);
					if (refreshed) {
						return;
					}

					heartbeatStopped = true;
					requestDrain();
				})
				.catch((error) => {
					if (!isRetryableSurrealWriteConflict(error)) {
						reportBackgroundError(
							`failed to refresh lease for ${row.messageId}`,
							error
						);
					}
				});
		}, heartbeatMs);

		try {
			if (closed) {
				await requeueJob(activeRow.messageId, 0, false);
				return;
			}

			const result = await executeMessageOverHttp(activeRow);
			await stopHeartbeat();

			if (result.type === 'completed') {
				await completeJob(activeRow.messageId);
				return;
			}

			if (result.type === 'reschedule') {
				const rescheduled = await requeueJob(
					activeRow.messageId,
					result.timeoutSeconds * 1000,
					true
				);
				if (rescheduled) {
					noteDelayedWork(result.timeoutSeconds * 1000);
				}
				return;
			}

			const retried = await retryJob(activeRow.messageId, activeRow.attempt);
			if (retried) {
				noteDelayedWork(computeRetryDelayMs(activeRow.attempt));
			}

			if (!shouldSuppressQueueExecutionError(result.text)) {
				console.error(
					`[world-surreal] Queue execution failed (${result.status}): ${result.text}`
				);
			}
		} finally {
			await stopHeartbeat();
			inflight.delete(row.messageId);
		}
	}

	async function dispatchClaimedJob(row: ClaimedQueueJobRow): Promise<void> {
		const release = await semaphore.acquire();

		const execute = async () => {
			try {
				await processClaimedJob(row);
			} catch (error) {
				reportBackgroundError(
					`failed to process queue job ${row.messageId}`,
					error
				);
			} finally {
				release();
				requestDrain();
			}
		};

		const workflowKey = row.workflowRunId
			? `workflow:${row.workflowRunId}`
			: undefined;
		const promise =
			workflowKey && row.queuePrefix === '__wkf_workflow_'
				? (inflightWorkflowRuns.get(workflowKey) ?? Promise.resolve())
						.catch(() => {})
						.then(async () => {
							await execute();
						})
						.finally(() => {
							if (inflightWorkflowRuns.get(workflowKey) === promise) {
								inflightWorkflowRuns.delete(workflowKey);
							}
						})
				: execute();

		inflight.set(row.messageId, promise);
		if (workflowKey) {
			inflightWorkflowRuns.set(workflowKey, promise);
		}
	}

	async function handleLiveNotification(
		notification: LiveNotification<QueueLaneRow>
	) {
		if ((notification.result?.readyCount || 0) <= 0) {
			return;
		}

		requestDrain();
	}

	async function subscribeToQueueFeed() {
		liveFeed = await liveClient.liveTable<QueueLaneRow>(
			Tables.queueLanes,
			{
				fields: [
					'laneKey',
					'queueName',
					'queuePrefix',
					'queueId',
					'queuedCount',
					'readyCount',
					'processingCount',
					'nextAvailableAt',
					'updatedAt',
				],
				where: gt('readyCount', 0),
			},
			(notification) => {
				void handleLiveNotification(notification).catch((error) => {
					reportBackgroundError(
						'failed to handle queue live notification',
						error
					);
				});
			}
		);
	}

	async function recoverLiveFeed() {
		while (!closed) {
			try {
				await liveClient.reconnect();
				await subscribeToQueueFeed();
				requestDrain();
				return;
			} catch (error) {
				if (closed) {
					return;
				}

				reportBackgroundError('failed to recover live feed', error);
				await new Promise<void>((resolve) => {
					globalThis.setTimeout(resolve, liveReconnectDelayMs);
				});
			}
		}
	}

	async function start(): Promise<void> {
		if (!startPromise) {
			startPromise = (async () => {
				await ensureSchema();
				await liveClient.connect();
				await subscribeToQueueFeed();
				requestDrain();
				unsubscribeClose = liveClient.onClose(() => {
					void liveFeed?.close().catch(() => {});
					liveFeed = null;
					clearWakeTimer();
					void recoverLiveFeed().catch((error) => {
						reportBackgroundError('failed to restart live feed', error);
					});
				});
			})();
		}

		await startPromise;
	}

	const queue: Queue['queue'] = async (queueName, message, opts) => {
		await ensureSchema();
		await start();
		QueuePayloadSchema.parse(message);

		const body = toBinaryPayload(transport.serialize(message));
		const [queuePrefix, queueId] = parseQueueName(queueName);
		const workflowInvoke = WorkflowInvokePayloadSchema.safeParse(message);
		const messageId = MessageId.parse(`msg_${generateMessageId()}`);
		const laneKey = buildLaneKey({
			messageId,
			queueName,
			queuePrefix,
			workflowRunId: workflowInvoke.success
				? workflowInvoke.data.runId
				: undefined,
		});
		let queuedMessageId: string = messageId;

		if (opts?.idempotencyKey) {
			const delayMs = (opts?.delaySeconds || 0) * 1000;
			const queueVars = {
				recordKey: messageId,
				messageId,
				laneKey,
				queueName,
				queuePrefix,
				queueId,
				body,
				delay: durationValueFromMs(delayMs),
				idempotencyKey: opts.idempotencyKey,
				...(workflowInvoke.success
					? { workflowRunId: workflowInvoke.data.runId }
					: {}),
				...(opts.headers ? { headers: opts.headers } : {}),
			};
			queuedMessageId =
				(await retryQueueMutation(async () =>
					callQueueFunction<string | null>(
						'fn::workflow::queue::enqueue_dedupe',
						[
							queueVars.recordKey,
							queueVars.messageId,
							queueVars.laneKey,
							queueVars.queueName,
							queueVars.queuePrefix,
							queueVars.queueId,
							queueVars.workflowRunId,
							queueVars.body,
							queueVars.headers,
							queueVars.idempotencyKey,
							queueVars.delay,
						]
					)
				)) ?? messageId;
			noteDelayedWork(delayMs);
		} else {
			const delayMs = (opts?.delaySeconds || 0) * 1000;
			await retryQueueMutation(async () =>
				callQueueFunction<string | null>(
					'fn::workflow::queue::enqueue_simple',
					[
						messageId,
						messageId,
						laneKey,
						queueName,
						queuePrefix,
						queueId,
						workflowInvoke.success ? workflowInvoke.data.runId : undefined,
						body,
						opts?.headers,
						durationValueFromMs(delayMs),
					]
				)
			);
			noteDelayedWork(delayMs);
		}

		requestDrain();
		return {
			messageId: MessageId.parse(queuedMessageId),
		};
	};

	const getDeploymentId: Queue['getDeploymentId'] = async () => 'surreal';

	return {
		createQueueHandler: localWorld.createQueueHandler,
		getDeploymentId,
		queue,
		start,
		async close() {
			closed = true;
			clearWakeTimer();
			clearTerminalFlushTimer();
			await liveFeed?.close().catch(() => {});
			liveFeed = null;
			unsubscribeClose?.();
			await flushTerminalMutations().catch(() => {});
			await Promise.allSettled([...inflight.values()]);
			startPromise = null;
			await liveClient.close();
			await localWorld.close?.();
		},
	};
}
