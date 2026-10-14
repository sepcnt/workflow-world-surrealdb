import {
	EntityConflictError,
	HookNotFoundError,
	RunExpiredError,
	RunNotSupportedError,
	TooEarlyError,
	WorkflowRunNotFoundError,
	WorkflowWorldError,
} from '@workflow/errors';
import type {
	AnyEventRequest,
	CreateEventParams,
	Event,
	EventResult,
	EventsResolveData,
	GetEventParams,
	Hook,
	ListEventsParams,
	PaginatedResponse,
	SpecVersion,
	Step,
	Storage,
	Wait,
	WorkflowRun,
} from '@workflow/world';
import {
	applyAttributeChanges,
	EventSchema,
	eventIdToSlot,
	FIRST_EVENT_SLOT,
	getMaxEventsPerRun,
	HookSchema,
	isChildEntityCreationEvent,
	isHookEventRequiringExistence,
	isTerminalRunEventType,
	isTerminalStepStatus,
	isTerminalWorkflowRunStatus,
	mintedSpecVersion,
	requiresNewerWorld,
	SPEC_VERSION_CURRENT,
	slotToEventId,
	validateAttributeChanges,
	validateUlidTimestamp,
} from '@workflow/world';
import { DEFAULT_RESOLVE_DATA_OPTION, Tables } from './constants.js';
import { stripEventDataRefs } from './filters.js';
import { createHooksStorage } from './hooks-storage.js';
import {
	deserializeEventRow,
	deserializeHookRow,
	type EventRow,
	type HookRow,
	queryFirstRow,
	queryRows,
	serializeEventRow,
	serializeHookRow,
	serializeRunStored,
	serializeStepRow,
	serializeWaitRow,
} from './records.js';
import type { SurrealRpcClient } from './rpc.js';
import { createRunsStorage } from './runs-storage.js';
import { createStepsStorage } from './steps-storage.js';
import {
	createLockMap,
	type EnsureSchema,
	getHook,
	getRun,
	getStep,
	getWait,
} from './storage-helpers.js';
import { coerceErrorMessage, ids } from './util.js';

const STEP_EVENT_TYPES = new Set([
	'step_created',
	'step_started',
	'step_completed',
	'step_failed',
	'step_retrying',
]);

/**
 * Bounds for the optimistic commit loop in `events.create`. A loser retries
 * immediately a few times (a parallel fan-out is the common source of
 * contention and resolves fast), then backs off with jitter.
 */
const COMMIT_MAX_ATTEMPTS = 64;
const COMMIT_IMMEDIATE_ATTEMPTS = 4;
const COMMIT_MAX_BACKOFF_MS = 50;

/**
 * A write lost a race the store settled: the event slot it chose, an entity
 * record or a unique index was taken by a concurrent writer, or SurrealDB
 * aborted the transaction on a conflict. Nothing from the attempt was
 * persisted, so the whole create is re-derived from fresh reads.
 */
class CommitConflict extends Error {}

function isCommitConflictMessage(message: string): boolean {
	return /already exists|already contains|transaction conflict|write conflict|retry the transaction|can be retried/i.test(
		message
	);
}

type Mutation =
	| {
			op: 'create' | 'upsert';
			table: string;
			key: string;
			value: unknown;
	  }
	| { op: 'delete'; table: string; key: string }
	| { op: 'deleteWhereRun'; table: string; runId: string };

/**
 * Applies a create's entity mutations and appends its events in a single
 * SurrealDB transaction.
 *
 * Event records are keyed `${runId}:${eventId}`, and `eventId` is the slot
 * position, so the record key is the uniqueness constraint on `(runId, slot)`:
 * a second writer for the same slot fails its `CREATE` and the transaction
 * rolls back, taking its entity mutations with it.
 */
async function commitTransaction(
	client: SurrealRpcClient,
	mutations: Mutation[],
	events: Event[]
): Promise<void> {
	const statements: string[] = ['BEGIN TRANSACTION;'];
	const vars: Record<string, unknown> = {};
	let n = 0;

	const push = (mutation: Mutation) => {
		const i = n++;
		vars[`t${i}`] = mutation.table;
		if (mutation.op === 'deleteWhereRun') {
			vars[`r${i}`] = mutation.runId;
			statements.push(`DELETE FROM type::table($t${i}) WHERE runId == $r${i};`);
			return;
		}
		vars[`k${i}`] = mutation.key;
		if (mutation.op === 'delete') {
			statements.push(`DELETE type::record($t${i}, $k${i});`);
			return;
		}
		vars[`v${i}`] = mutation.value;
		statements.push(
			`${mutation.op === 'create' ? 'CREATE' : 'UPSERT'} type::record($t${i}, $k${i}) CONTENT $v${i};`
		);
	};

	for (const mutation of mutations) {
		push(mutation);
	}
	for (const event of events) {
		push({
			op: 'create',
			table: Tables.events,
			key: `${event.runId}:${event.eventId}`,
			value: serializeEventRow(event),
		});
	}
	statements.push('COMMIT TRANSACTION;');

	try {
		await client.transaction(statements.join('\n'), vars);
	} catch (error) {
		const message = coerceErrorMessage(error);
		if (isCommitConflictMessage(message)) {
			throw new CommitConflict(message);
		}
		throw error;
	}
}

/**
 * The run's highest committed slot, or 0 for an empty log.
 *
 * Slot ids sort lexicographically in slot order because `slotToEventId`
 * pads to a fixed width. The range bound skips event ids from before slot
 * numbering (`wevt_<ulid>`), which do not encode a position.
 */
async function readHeadSlot(
	client: SurrealRpcClient,
	runId: string
): Promise<number> {
	const row = await queryFirstRow<{ eventId: string }>(
		client,
		`SELECT eventId FROM ${Tables.events} WHERE runId == $runId AND eventId >= $low AND eventId <= $high ORDER BY eventId DESC LIMIT 1;`,
		{
			runId,
			low: slotToEventId(FIRST_EVENT_SLOT),
			high: 'evnt_~',
		}
	);
	return row ? (eventIdToSlot(row.eventId) ?? 0) : 0;
}

async function sleepBackoff(attempt: number): Promise<void> {
	if (attempt < COMMIT_IMMEDIATE_ATTEMPTS) {
		return;
	}
	const ceiling = Math.min(
		COMMIT_MAX_BACKOFF_MS,
		2 ** (attempt - COMMIT_IMMEDIATE_ATTEMPTS)
	);
	await new Promise<void>((resolve) => {
		globalThis.setTimeout(resolve, Math.random() * ceiling);
	});
}

export { createHooksStorage, createRunsStorage, createStepsStorage };

export interface EventsStorageOptions {
	/**
	 * The spec version stamped on a run whose creation request carries none.
	 * Defaults to `mintedSpecVersion()`, read per storage instance.
	 */
	specVersion?: SpecVersion;
}

export function createEventsStorage(
	client: SurrealRpcClient,
	ensureSchema: EnsureSchema,
	options: EventsStorageOptions = {}
): Storage['events'] {
	const getLock = createLockMap();
	const worldSpecVersion = options.specVersion ?? mintedSpecVersion();

	async function listEvents(
		params: ListEventsParams
	): Promise<PaginatedResponse<Event>> {
		await ensureSchema();

		const limit = params?.pagination?.limit ?? 100;
		const sortOrder = params.pagination?.sortOrder || 'asc';
		const comparator = sortOrder === 'desc' ? '<' : '>';
		const direction = sortOrder === 'desc' ? 'DESC' : 'ASC';

		const listConditions: string[] = ['runId == $runId'];
		const listVars: Record<string, unknown> = {
			runId: params.runId,
			limit: limit + 1,
		};

		if (params.pagination?.cursor) {
			listConditions.push(`eventId ${comparator} $cursor`);
			listVars.cursor = params.pagination.cursor;
		}

		const rows = await queryRows<EventRow>(
			client,
			`SELECT * FROM ${Tables.events} WHERE ${listConditions.join(' AND ')} ORDER BY eventId ${direction} LIMIT $limit;`,
			listVars
		);

		const hasMore = rows.length > limit;
		const values = rows.slice(0, limit).map(deserializeEventRow);
		const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;

		return {
			data: values.map((event) => stripEventDataRefs(event, resolveData)),
			cursor: values.at(-1)?.eventId ?? null,
			hasMore,
		};
	}

	/**
	 * The report half of bump-and-report: the events occupying the slots
	 * between the one the writer expected (`eventCount + 1`) and the one its
	 * write committed at. No cursor is returned, because the report is not a
	 * page the caller has read to the end of and must not move its position.
	 */
	async function reportSkippedSlots(
		result: EventResult,
		eventCount: number,
		resolveData: EventsResolveData
	): Promise<EventResult> {
		if (!result.event) {
			return result;
		}
		const committedSlot = eventIdToSlot(result.event.eventId);
		if (committedSlot === null || committedSlot <= eventCount + 1) {
			return result;
		}
		const span = committedSlot - Math.max(eventCount, 0) - 1;
		const page = await listEvents({
			runId: result.event.runId,
			pagination: {
				...(eventCount >= FIRST_EVENT_SLOT
					? { cursor: slotToEventId(eventCount) }
					: {}),
				limit: span,
				sortOrder: 'asc',
			},
			resolveData,
		});
		const committedEventId = result.event.eventId;
		const events = page.data.filter(
			(event) => event.eventId < committedEventId
		);
		return {
			...result,
			events,
			cursor: null,
			hasMore: events.length < span,
		};
	}

	async function createOnce(
		effectiveRunId: string,
		data: AnyEventRequest,
		params: CreateEventParams | undefined
	): Promise<EventResult> {
		// The head is read before any entity state. Every create that changes
		// state also appends an event, so a writer that commits after this
		// read takes the slot this attempt is about to claim, and the attempt
		// fails and starts over from reads that include that writer's changes.
		const head = await readHeadSlot(client, effectiveRunId);
		const now = new Date();
		const effectiveSpecVersion = data.specVersion ?? worldSpecVersion;
		const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;

		const mutations: Mutation[] = [];
		const events: Event[] = [];
		const appendEvent = (fields: Record<string, unknown>): Event => {
			const event = EventSchema.parse({
				...fields,
				runId: effectiveRunId,
				eventId: slotToEventId(head + events.length + 1),
				createdAt: now,
				specVersion: effectiveSpecVersion,
			});
			events.push(event);
			return event;
		};

		let currentRun: WorkflowRun | null = null;
		if (data.eventType !== 'run_created') {
			currentRun = (await getRun(client, effectiveRunId)) ?? null;
		} else if (await getRun(client, effectiveRunId)) {
			throw new EntityConflictError(
				`Workflow run "${effectiveRunId}" already exists`
			);
		}

		// Resilient start: a run_started carrying the run's creation data on a
		// run that does not exist creates it, so the queue can bootstrap a run
		// whose `start()` failed to write `run_created`.
		if (
			data.eventType === 'run_started' &&
			!currentRun &&
			data.eventData?.deploymentId &&
			data.eventData.workflowName &&
			data.eventData.input !== undefined
		) {
			const runInput = data.eventData;
			validateAttributeChanges(
				Object.entries(runInput.attributes ?? {}).map(([key, value]) => ({
					key,
					value,
				})),
				{ allowReservedAttributes: runInput.allowReservedAttributes === true }
			);
			const createdRun = {
				runId: effectiveRunId,
				deploymentId: runInput.deploymentId as string,
				workflowName: runInput.workflowName as string,
				status: 'pending',
				specVersion: effectiveSpecVersion,
				executionContext: runInput.executionContext,
				input: runInput.input,
				attributes: runInput.attributes ?? {},
				encryptionPublicKey: runInput.encryptionPublicKey,
				createdAt: now,
				updatedAt: now,
			} as WorkflowRun;
			mutations.push({
				op: 'create',
				table: Tables.runs,
				key: effectiveRunId,
				value: serializeRunStored(createdRun),
			});
			appendEvent({
				eventType: 'run_created',
				eventData: {
					deploymentId: runInput.deploymentId,
					workflowName: runInput.workflowName,
					input: runInput.input,
					executionContext: runInput.executionContext,
					attributes: runInput.attributes,
					allowReservedAttributes: runInput.allowReservedAttributes,
					encryptionPublicKey: runInput.encryptionPublicKey,
				},
			});
			currentRun = createdRun;
		}

		if (
			!currentRun &&
			(data.eventType === 'run_failed' ||
				data.eventType === 'attr_set' ||
				data.eventType === 'run_started')
		) {
			throw new WorkflowRunNotFoundError(effectiveRunId);
		}

		if (currentRun && requiresNewerWorld(currentRun.specVersion)) {
			throw new RunNotSupportedError(
				currentRun.specVersion ?? 0,
				SPEC_VERSION_CURRENT
			);
		}

		const lazyStepStart =
			data.eventType === 'step_started' && isChildEntityCreationEvent(data);

		if (currentRun && isTerminalWorkflowRunStatus(currentRun.status)) {
			if (
				data.eventType === 'run_cancelled' &&
				currentRun.status === 'cancelled'
			) {
				const event = appendEvent(data);
				await commitTransaction(client, mutations, events);
				return {
					event: stripEventDataRefs(event, resolveData),
					run: currentRun,
					maxEvents: getMaxEventsPerRun(),
				};
			}
			if (data.eventType === 'run_started') {
				throw new RunExpiredError(
					`Workflow run "${effectiveRunId}" is already in terminal state "${currentRun.status}"`
				);
			}
			if (data.eventType === 'hook_received') {
				throw new RunExpiredError(
					`Workflow run "${effectiveRunId}" is already in a terminal state`
				);
			}
			if (isTerminalRunEventType(data.eventType)) {
				throw new EntityConflictError(
					`Cannot transition run from terminal state "${currentRun.status}"`
				);
			}
			if (isChildEntityCreationEvent(data)) {
				throw new EntityConflictError(
					`Cannot create new entities on run in terminal state "${currentRun.status}"`
				);
			}
			if (data.eventType === 'attr_set') {
				throw new EntityConflictError(
					`Cannot set attributes on run in terminal state "${currentRun.status}"`
				);
			}
		}

		let validatedStep: Step | null = null;
		if (
			STEP_EVENT_TYPES.has(data.eventType) &&
			data.eventType !== 'step_created' &&
			data.correlationId
		) {
			validatedStep =
				(await getStep(client, effectiveRunId, data.correlationId)) ?? null;

			if (!validatedStep && !lazyStepStart) {
				throw new WorkflowWorldError(`Step "${data.correlationId}" not found`);
			}
			// A lazy step_started always creates its step. Finding one means a
			// concurrent handler already won the creation, and this caller must
			// not start or run the step.
			if (lazyStepStart && validatedStep) {
				throw new EntityConflictError(
					`Step "${data.correlationId}" already created`
				);
			}
			if (validatedStep) {
				if (isTerminalStepStatus(validatedStep.status)) {
					throw new EntityConflictError(
						`Cannot modify step in terminal state "${validatedStep.status}"`
					);
				}
				// On a terminal run only a step already in flight may record its
				// outcome. A step_started never qualifies, even when the step row
				// reads `running`: that is a redelivery of a start an earlier
				// delivery claimed, and its body's outcome would never be read.
				if (
					currentRun &&
					isTerminalWorkflowRunStatus(currentRun.status) &&
					(validatedStep.status !== 'running' ||
						data.eventType === 'step_started')
				) {
					throw new RunExpiredError(
						`Cannot ${data.eventType === 'step_started' ? 'start' : 'modify non-running'} step on run in terminal state "${currentRun.status}"`
					);
				}
			}
		}

		let validatedHook: Hook | null = null;
		if (isHookEventRequiringExistence(data.eventType) && data.correlationId) {
			validatedHook = (await getHook(client, data.correlationId)) ?? null;
			if (!validatedHook) {
				throw new HookNotFoundError(data.correlationId);
			}
		}

		let run: WorkflowRun | undefined;
		let step: Step | undefined;
		let hook: Hook | undefined;
		let wait: Wait | undefined;
		let stepCreated = false;

		const writeRun = (next: WorkflowRun) => {
			run = next;
			mutations.push({
				op: 'upsert',
				table: Tables.runs,
				key: effectiveRunId,
				value: serializeRunStored(next),
			});
		};
		const writeStep = (next: Step, op: 'create' | 'upsert' = 'upsert') => {
			step = next;
			mutations.push({
				op,
				table: Tables.steps,
				key: `${effectiveRunId}:${next.stepId}`,
				value: serializeStepRow(next),
			});
		};
		const releaseRunEntities = () => {
			mutations.push(
				{ op: 'deleteWhereRun', table: Tables.hooks, runId: effectiveRunId },
				{ op: 'deleteWhereRun', table: Tables.waits, runId: effectiveRunId }
			);
		};

		// The event as journaled. run_started's creation data belongs on
		// run_created only, and a lazy step_started's input belongs on the
		// step_created written for it.
		let eventFields: Record<string, unknown> = { ...data };
		if (data.eventType === 'run_started') {
			delete eventFields.eventData;
		}
		if (data.eventType === 'run_created' && data.eventData) {
			const {
				dynamicWorkflowCode: _code,
				dynamicWorkflowCodeRef: _ref,
				...eventData
			} = data.eventData;
			eventFields = { ...eventFields, eventData };
		}
		if (lazyStepStart && data.eventData) {
			const { input: _input, ...eventData } = data.eventData;
			eventFields = { ...eventFields, eventData };
		}

		if (data.eventType === 'run_created') {
			const runData = data.eventData;
			validateAttributeChanges(
				Object.entries(runData.attributes ?? {}).map(([key, value]) => ({
					key,
					value,
				})),
				{ allowReservedAttributes: runData.allowReservedAttributes === true }
			);
			const nextRun = {
				runId: effectiveRunId,
				deploymentId: runData.deploymentId,
				workflowName: runData.workflowName,
				status: 'pending',
				specVersion: effectiveSpecVersion,
				executionContext: runData.executionContext,
				input: runData.input,
				attributes: runData.attributes ?? {},
				encryptionPublicKey: runData.encryptionPublicKey,
				createdAt: now,
				updatedAt: now,
			} as WorkflowRun;
			run = nextRun;
			mutations.push({
				op: 'create',
				table: Tables.runs,
				key: effectiveRunId,
				value: serializeRunStored(nextRun),
			});
		} else if (data.eventType === 'run_started' && currentRun) {
			// run_started is a get-or-start: the runtime sends it on every
			// invocation, but the log holds exactly one. A running run answers
			// without writing, and so without an event, since a returned event
			// would name a slot nothing occupies.
			if (currentRun.status === 'running') {
				return { run: currentRun, maxEvents: getMaxEventsPerRun() };
			} else {
				writeRun({
					...currentRun,
					status: 'running',
					startedAt: currentRun.startedAt ?? now,
					updatedAt: now,
					output: undefined,
					error: undefined,
					completedAt: undefined,
				} as WorkflowRun);
			}
		} else if (data.eventType === 'run_completed' && currentRun) {
			writeRun({
				...currentRun,
				status: 'completed',
				output: data.eventData?.output,
				error: undefined,
				completedAt: now,
				updatedAt: now,
			} as WorkflowRun);
			releaseRunEntities();
		} else if (data.eventType === 'run_failed' && currentRun) {
			writeRun({
				...currentRun,
				status: 'failed',
				output: undefined,
				error: data.eventData.error,
				errorCode: data.eventData.errorCode,
				completedAt: now,
				updatedAt: now,
			} as WorkflowRun);
			releaseRunEntities();
		} else if (data.eventType === 'run_cancelled' && currentRun) {
			writeRun({
				...currentRun,
				status: 'cancelled',
				output: undefined,
				error: undefined,
				completedAt: now,
				updatedAt: now,
			} as WorkflowRun);
			releaseRunEntities();
		} else if (data.eventType === 'attr_set' && currentRun) {
			validateAttributeChanges(data.eventData.changes, {
				existingKeys: Object.keys(currentRun.attributes ?? {}),
				allowReservedAttributes:
					data.eventData.allowReservedAttributes === true,
			});
			writeRun({
				...currentRun,
				attributes: applyAttributeChanges(
					currentRun.attributes ?? {},
					data.eventData.changes
				),
				updatedAt: now,
			} as WorkflowRun);
		} else if (data.eventType === 'step_created') {
			if (await getStep(client, effectiveRunId, data.correlationId)) {
				throw new EntityConflictError(
					`Step "${data.correlationId}" already created`
				);
			}
			writeStep(
				{
					runId: effectiveRunId,
					stepId: data.correlationId,
					stepName: data.eventData.stepName,
					status: 'pending',
					input: data.eventData.input,
					attempt: 0,
					createdAt: now,
					updatedAt: now,
					specVersion: effectiveSpecVersion,
				},
				'create'
			);
		} else if (data.eventType === 'step_started') {
			if (!validatedStep && lazyStepStart && data.eventData) {
				const stepName = data.eventData.stepName as string;
				validatedStep = {
					runId: effectiveRunId,
					stepId: data.correlationId,
					stepName,
					status: 'pending',
					input: data.eventData.input,
					attempt: 0,
					createdAt: now,
					updatedAt: now,
					specVersion: effectiveSpecVersion,
				};
				appendEvent({
					eventType: 'step_created',
					correlationId: data.correlationId,
					eventData: { stepName, input: data.eventData.input },
				});
				stepCreated = true;
			}
			if (validatedStep) {
				if (
					validatedStep.retryAfter &&
					validatedStep.retryAfter.getTime() > Date.now()
				) {
					throw new TooEarlyError(
						`Cannot start step "${data.correlationId}": retryAfter timestamp has not been reached yet`,
						{
							retryAfter: Math.ceil(
								(validatedStep.retryAfter.getTime() - Date.now()) / 1000
							),
						}
					);
				}
				writeStep(
					{
						...validatedStep,
						status: 'running',
						startedAt: validatedStep.startedAt ?? now,
						attempt: validatedStep.attempt + 1,
						retryAfter: undefined,
						updatedAt: now,
					},
					stepCreated ? 'create' : 'upsert'
				);
			}
		} else if (data.eventType === 'step_completed' && validatedStep) {
			writeStep({
				...validatedStep,
				status: 'completed',
				output: data.eventData.result,
				completedAt: now,
				updatedAt: now,
			});
		} else if (data.eventType === 'step_failed' && validatedStep) {
			writeStep({
				...validatedStep,
				status: 'failed',
				error: data.eventData.error,
				completedAt: now,
				updatedAt: now,
			});
		} else if (data.eventType === 'step_retrying' && validatedStep) {
			writeStep({
				...validatedStep,
				status: 'pending',
				error: data.eventData.error,
				retryAfter: data.eventData.retryAfter,
				updatedAt: now,
			});
		} else if (data.eventType === 'hook_created') {
			if (await getHook(client, data.correlationId)) {
				throw new EntityConflictError(
					`Hook "${data.correlationId}" already exists`
				);
			}
			const holder = await queryFirstRow<HookRow>(
				client,
				`SELECT * FROM ${Tables.hooks} WHERE tokenHash == crypto::sha256($hookToken) LIMIT 1;`,
				{ hookToken: data.eventData.token }
			);
			if (holder) {
				// The token is held: the creation is journaled as hook_conflict at
				// the slot hook_created would have taken.
				const event = appendEvent({
					eventType: 'hook_conflict',
					correlationId: data.correlationId,
					eventData: {
						token: data.eventData.token,
						conflictingRunId: deserializeHookRow(holder).runId,
					},
				});
				await commitTransaction(client, mutations, events);
				return { event: stripEventDataRefs(event, resolveData) };
			}
			hook = HookSchema.parse({
				runId: effectiveRunId,
				hookId: data.correlationId,
				token: data.eventData.token,
				metadata: data.eventData.metadata,
				ownerId: '',
				projectId: '',
				environment: '',
				createdAt: now,
				specVersion: effectiveSpecVersion,
				isWebhook: data.eventData.isWebhook,
				isSystem: data.eventData.isSystem,
				tokenRetentionUntil: data.eventData.tokenRetentionUntil,
			});
			// The tokenHash index is UNIQUE, so two runs creating the same token
			// concurrently cannot both commit; the loser's retry sees the holder
			// and journals hook_conflict.
			mutations.push({
				op: 'create',
				table: Tables.hooks,
				key: hook.hookId,
				value: serializeHookRow(hook),
			});
		} else if (data.eventType === 'hook_disposed' && validatedHook) {
			mutations.push({
				op: 'delete',
				table: Tables.hooks,
				key: validatedHook.hookId,
			});
		} else if (data.eventType === 'wait_created') {
			const waitId = `${effectiveRunId}-${data.correlationId}`;
			if (await getWait(client, `${effectiveRunId}:${waitId}`)) {
				throw new EntityConflictError(
					`Wait "${data.correlationId}" already exists`
				);
			}
			wait = {
				waitId,
				runId: effectiveRunId,
				status: 'waiting',
				resumeAt: data.eventData.resumeAt,
				createdAt: now,
				updatedAt: now,
				specVersion: effectiveSpecVersion,
			};
			mutations.push({
				op: 'create',
				table: Tables.waits,
				key: `${effectiveRunId}:${waitId}`,
				value: serializeWaitRow(wait),
			});
		} else if (data.eventType === 'wait_completed') {
			const waitId = `${effectiveRunId}-${data.correlationId}`;
			const existing = await getWait(client, `${effectiveRunId}:${waitId}`);
			if (!existing) {
				throw new WorkflowWorldError(`Wait "${data.correlationId}" not found`);
			}
			if (existing.status === 'completed') {
				throw new EntityConflictError(
					`Wait "${data.correlationId}" already completed`
				);
			}
			wait = {
				...existing,
				status: 'completed',
				completedAt: now,
				updatedAt: now,
			};
			mutations.push({
				op: 'upsert',
				table: Tables.waits,
				key: `${effectiveRunId}:${waitId}`,
				value: serializeWaitRow(wait),
			});
		}

		const event = appendEvent(eventFields);
		await commitTransaction(client, mutations, events);

		const resultRun = run as WorkflowRun | undefined;
		return {
			event: stripEventDataRefs(event, resolveData),
			run: resultRun,
			step,
			hook,
			wait,
			...(stepCreated ? { stepCreated: true as const } : {}),
			...(resultRun ? { maxEvents: getMaxEventsPerRun() } : {}),
		};
	}

	return {
		async create(
			runId: string | null,
			data: AnyEventRequest,
			params?: CreateEventParams
		): Promise<EventResult> {
			await ensureSchema();

			let effectiveRunId: string;
			if (data.eventType === 'run_created' && (!runId || runId === '')) {
				effectiveRunId = `wrun_${ids().runId()}`;
			} else if (!runId) {
				throw new Error('runId is required for non-run_created events');
			} else {
				effectiveRunId = runId;
			}

			if (data.eventType === 'run_created' && runId) {
				const validationError = validateUlidTimestamp(effectiveRunId, 'wrun_');
				if (validationError) {
					throw new WorkflowWorldError(validationError);
				}
			}

			const result = await getLock(effectiveRunId).andThen(async () => {
				for (let attempt = 0; ; attempt++) {
					try {
						return await createOnce(effectiveRunId, data, params);
					} catch (error) {
						if (
							!(error instanceof CommitConflict) ||
							attempt >= COMMIT_MAX_ATTEMPTS - 1
						) {
							if (error instanceof CommitConflict) {
								throw new WorkflowWorldError(
									`Could not append ${data.eventType} to run "${effectiveRunId}" after ${COMMIT_MAX_ATTEMPTS} attempts: ${error.message}`
								);
							}
							throw error;
						}
						await sleepBackoff(attempt);
					}
				}
			});

			if (params?.eventCount === undefined) {
				return result;
			}
			return reportSkippedSlots(
				result,
				params.eventCount,
				params.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION
			);
		},

		async get(
			runId: string,
			eventId: string,
			params?: GetEventParams
		): Promise<Event> {
			await ensureSchema();

			const row = await queryFirstRow<EventRow>(
				client,
				`SELECT * FROM ${Tables.events} WHERE runId == $runId AND eventId == $eventId LIMIT 1;`,
				{ runId, eventId }
			);
			if (!row) {
				throw new WorkflowWorldError(`Event not found: ${eventId}`);
			}

			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;
			return stripEventDataRefs(deserializeEventRow(row), resolveData);
		},

		list: listEvents,

		async listByCorrelationId(params) {
			await ensureSchema();

			const limit = params?.pagination?.limit ?? 100;
			const sortOrder = params.pagination?.sortOrder || 'asc';
			const comparator = sortOrder === 'desc' ? '<' : '>';
			const direction = sortOrder === 'desc' ? 'DESC' : 'ASC';

			// A correlation id names a step, hook or wait within one run, so
			// the lookup is always scoped to it.
			const corrConditions: string[] = [
				'runId == $runId',
				'correlationId == $correlationId',
			];
			const corrVars: Record<string, unknown> = {
				runId: params.runId,
				correlationId: params.correlationId,
				limit: limit + 1,
			};

			if (params.pagination?.cursor) {
				corrConditions.push(`eventId ${comparator} $cursor`);
				corrVars.cursor = params.pagination.cursor;
			}

			const rows = await queryRows<EventRow>(
				client,
				`SELECT * FROM ${Tables.events} WHERE ${corrConditions.join(' AND ')} ORDER BY eventId ${direction} LIMIT $limit;`,
				corrVars
			);

			const hasMore = rows.length > limit;
			const values = rows.slice(0, limit).map(deserializeEventRow);
			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;

			return {
				data: values.map((event) => stripEventDataRefs(event, resolveData)),
				cursor: values.at(-1)?.eventId ?? null,
				hasMore,
			};
		},
	} as Storage['events'];
}
