import {
	EntityConflictError,
	HookNotFoundError,
	RunExpiredError,
	RunNotSupportedError,
	TooEarlyError,
	WorkflowWorldError,
} from '@workflow/errors';
import type {
	Event,
	EventResult,
	GetEventParams,
	Hook,
	ListEventsParams,
	PaginatedResponse,
	Step,
	Storage,
	Wait,
	WorkflowRun,
} from '@workflow/world';
import {
	EventSchema,
	HookSchema,
	SPEC_VERSION_CURRENT,
	validateUlidTimestamp,
} from '@workflow/world';
import { monotonicFactory } from 'ulid';
import { DEFAULT_RESOLVE_DATA_OPTION, Tables } from './constants.js';
import { stripEventDataRefs } from './filters.js';
import { createHooksStorage } from './hooks-storage.js';
import {
	createThing,
	deleteThing,
	deserializeEventRow,
	type EventRow,
	type HookRow,
	queryFirstRow,
	queryRows,
	serializeEventRow,
	serializeHookRow,
	serializeRunStored,
	serializeStepRow,
	serializeWaitRow,
	upsertThing,
} from './records.js';
import type { SurrealRpcClient } from './rpc.js';
import { createRunsStorage } from './runs-storage.js';
import { createStepsStorage } from './steps-storage.js';
import {
	createLockMap,
	deleteAllHooksForRun,
	deleteAllWaitsForRun,
	type EnsureSchema,
	getHook,
	getRun,
	getStep,
	getWait,
} from './storage-helpers.js';

const ulid = monotonicFactory();

function assertSupportedSpecVersion(run: WorkflowRun): void {
	if (run.specVersion !== SPEC_VERSION_CURRENT) {
		throw new RunNotSupportedError(run.specVersion ?? 0, SPEC_VERSION_CURRENT);
	}
}

function isRunTerminal(status: WorkflowRun['status']) {
	return ['completed', 'failed', 'cancelled'].includes(status);
}

function isStepTerminal(status: Step['status']) {
	return ['completed', 'failed', 'cancelled'].includes(status);
}

function toStructuredError(
	value: unknown,
	fallbackCode?: string
): NonNullable<WorkflowRun['error']> {
	if (typeof value === 'string') {
		return { message: value, code: fallbackCode };
	}

	const objectValue =
		value && typeof value === 'object'
			? (value as Record<string, unknown>)
			: undefined;
	return {
		message:
			typeof objectValue?.message === 'string'
				? objectValue.message
				: 'Unknown error',
		stack:
			typeof objectValue?.stack === 'string' ? objectValue.stack : undefined,
		code:
			typeof objectValue?.code === 'string' ? objectValue.code : fallbackCode,
	};
}

async function createHookConflictEvent(
	client: SurrealRpcClient,
	runId: string,
	eventId: string,
	correlationId: string | undefined,
	token: string,
	createdAt: Date
): Promise<Event> {
	const event = EventSchema.parse({
		eventType: 'hook_conflict',
		correlationId,
		eventData: { token },
		runId,
		eventId,
		createdAt,
		specVersion: SPEC_VERSION_CURRENT,
	});

	await createThing<EventRow>(
		client,
		Tables.events,
		`${runId}:${eventId}`,
		serializeEventRow(event)
	);

	return event;
}

export { createHooksStorage, createRunsStorage, createStepsStorage };

export function createEventsStorage(
	client: SurrealRpcClient,
	ensureSchema: EnsureSchema
): Storage['events'] {
	const getLock = createLockMap();

	return {
		async create(runId, data, params): Promise<EventResult> {
			await ensureSchema();

			const effectiveRunId =
				data.eventType === 'run_created'
					? runId && runId !== ''
						? runId
						: `wrun_${ulid()}`
					: runId;

			if (!effectiveRunId) {
				throw new Error('runId is required for non-run_created events');
			}

			return getLock(effectiveRunId).andThen(async () => {
				const now = new Date();
				const eventId = `wevt_${ulid()}`;
				const effectiveSpecVersion = data.specVersion ?? SPEC_VERSION_CURRENT;
				const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;

				if (data.eventType === 'run_created' && runId) {
					const validationError = validateUlidTimestamp(
						effectiveRunId,
						'wrun_'
					);
					if (validationError) {
						throw new WorkflowWorldError(validationError);
					}
				}

				const currentRun =
					data.eventType === 'run_created'
						? null
						: ((await getRun(client, effectiveRunId)) ?? null);

				if (currentRun) {
					assertSupportedSpecVersion(currentRun);
				}

				// Runtime dispatches `run_started` on every step checkpoint as a
				// get-or-start, but event sourcing requires exactly one per run.
				// Short-circuit duplicates once the run is already running.
				if (
					data.eventType === 'run_started' &&
					currentRun &&
					currentRun.status === 'running'
				) {
					const event = EventSchema.parse({
						...data,
						runId: effectiveRunId,
						eventId,
						createdAt: currentRun.startedAt ?? now,
						specVersion: effectiveSpecVersion,
					});
					return {
						event: stripEventDataRefs(event, resolveData),
						run: currentRun,
					};
				}

				if (currentRun && isRunTerminal(currentRun.status)) {
					if (
						data.eventType === 'run_cancelled' &&
						currentRun.status === 'cancelled'
					) {
						const event = EventSchema.parse({
							...data,
							runId: effectiveRunId,
							eventId,
							createdAt: now,
							specVersion: effectiveSpecVersion,
						});

						await createThing<EventRow>(
							client,
							Tables.events,
							`${effectiveRunId}:${eventId}`,
							serializeEventRow(event)
						);

						return {
							event: stripEventDataRefs(event, resolveData),
							run: currentRun,
						};
					}

					if (
						data.eventType === 'run_started' ||
						data.eventType === 'run_completed' ||
						data.eventType === 'run_failed' ||
						data.eventType === 'run_cancelled'
					) {
						throw new EntityConflictError(
							`Cannot transition run from terminal state "${currentRun.status}"`
						);
					}

					if (
						data.eventType === 'step_created' ||
						data.eventType === 'hook_created' ||
						data.eventType === 'wait_created'
					) {
						throw new EntityConflictError(
							`Cannot create new entities on run in terminal state "${currentRun.status}"`
						);
					}
				}

				let validatedStep: Step | null = null;
				if (
					[
						'step_started',
						'step_completed',
						'step_failed',
						'step_retrying',
					].includes(data.eventType) &&
					data.correlationId
				) {
					validatedStep =
						(await getStep(client, effectiveRunId, data.correlationId)) ?? null;

					if (!validatedStep) {
						throw new WorkflowWorldError(
							`Step "${data.correlationId}" not found`
						);
					}

					if (isStepTerminal(validatedStep.status)) {
						throw new EntityConflictError(
							`Cannot modify step in terminal state "${validatedStep.status}"`
						);
					}

					if (
						currentRun &&
						isRunTerminal(currentRun.status) &&
						validatedStep.status !== 'running'
					) {
						throw new RunExpiredError(
							`Cannot modify non-running step on run in terminal state "${currentRun.status}"`
						);
					}
				}

				let validatedHook: Hook | null = null;
				if (
					['hook_disposed', 'hook_received'].includes(data.eventType) &&
					data.correlationId
				) {
					validatedHook = (await getHook(client, data.correlationId)) ?? null;
					if (!validatedHook) {
						throw new HookNotFoundError(data.correlationId);
					}
				}

				const event = EventSchema.parse({
					...data,
					runId: effectiveRunId,
					eventId,
					createdAt: now,
					specVersion: effectiveSpecVersion,
				});

				let run: WorkflowRun | undefined;
				let step: Step | undefined;
				let hook: Hook | undefined;
				let wait: Wait | undefined;

				if (data.eventType === 'run_created' && 'eventData' in data) {
					const nextRun: WorkflowRun = {
						runId: effectiveRunId,
						deploymentId: data.eventData.deploymentId,
						workflowName: data.eventData.workflowName,
						input: data.eventData.input,
						executionContext: data.eventData.executionContext,
						status: 'pending',
						createdAt: now,
						updatedAt: now,
						output: undefined,
						error: undefined,
						startedAt: undefined,
						completedAt: undefined,
						specVersion: SPEC_VERSION_CURRENT,
					};
					run = nextRun;

					await createThing(
						client,
						Tables.runs,
						effectiveRunId,
						serializeRunStored(nextRun)
					);
				} else if (data.eventType === 'run_started' && currentRun) {
					const nextRun = {
						...currentRun,
						status: 'running',
						startedAt: currentRun.startedAt ?? now,
						updatedAt: now,
						output: undefined,
						error: undefined,
						completedAt: undefined,
					} as WorkflowRun;
					run = nextRun;

					await upsertThing(
						client,
						Tables.runs,
						effectiveRunId,
						serializeRunStored(nextRun)
					);
				} else if (data.eventType === 'run_completed' && currentRun) {
					const nextRun = {
						...currentRun,
						status: 'completed',
						output: data.eventData?.output,
						error: undefined,
						completedAt: now,
						updatedAt: now,
					} as WorkflowRun;
					run = nextRun;

					await upsertThing(
						client,
						Tables.runs,
						effectiveRunId,
						serializeRunStored(nextRun)
					);
					await Promise.all([
						deleteAllHooksForRun(client, effectiveRunId),
						deleteAllWaitsForRun(client, effectiveRunId),
					]);
				} else if (data.eventType === 'run_failed' && currentRun) {
					const nextRun = {
						...currentRun,
						status: 'failed',
						output: undefined,
						error: toStructuredError(
							data.eventData?.error,
							data.eventData?.errorCode
						),
						completedAt: now,
						updatedAt: now,
					} as WorkflowRun;
					run = nextRun;

					await upsertThing(
						client,
						Tables.runs,
						effectiveRunId,
						serializeRunStored(nextRun)
					);
					await Promise.all([
						deleteAllHooksForRun(client, effectiveRunId),
						deleteAllWaitsForRun(client, effectiveRunId),
					]);
				} else if (data.eventType === 'run_cancelled' && currentRun) {
					const nextRun = {
						...currentRun,
						status: 'cancelled',
						output: undefined,
						error: undefined,
						completedAt: now,
						updatedAt: now,
					} as WorkflowRun;
					run = nextRun;

					await upsertThing(
						client,
						Tables.runs,
						effectiveRunId,
						serializeRunStored(nextRun)
					);
					await Promise.all([
						deleteAllHooksForRun(client, effectiveRunId),
						deleteAllWaitsForRun(client, effectiveRunId),
					]);
				} else if (data.eventType === 'step_created' && 'eventData' in data) {
					step = {
						runId: effectiveRunId,
						stepId: data.correlationId!,
						stepName: data.eventData.stepName,
						status: 'pending',
						input: data.eventData.input,
						output: undefined,
						error: undefined,
						attempt: 0,
						startedAt: undefined,
						completedAt: undefined,
						createdAt: now,
						updatedAt: now,
						specVersion: SPEC_VERSION_CURRENT,
					};

					await createThing(
						client,
						Tables.steps,
						`${effectiveRunId}:${step.stepId}`,
						serializeStepRow(step)
					);
				} else if (data.eventType === 'step_started' && validatedStep) {
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

					step = {
						...validatedStep,
						status: 'running',
						startedAt: validatedStep.startedAt ?? now,
						attempt: validatedStep.attempt + 1,
						retryAfter: undefined,
						updatedAt: now,
					};

					await upsertThing(
						client,
						Tables.steps,
						`${effectiveRunId}:${step.stepId}`,
						serializeStepRow(step)
					);
				} else if (data.eventType === 'step_completed' && validatedStep) {
					step = {
						...validatedStep,
						status: 'completed',
						output: data.eventData?.result,
						completedAt: now,
						updatedAt: now,
					};

					await upsertThing(
						client,
						Tables.steps,
						`${effectiveRunId}:${step.stepId}`,
						serializeStepRow(step)
					);
				} else if (data.eventType === 'step_failed' && validatedStep) {
					step = {
						...validatedStep,
						status: 'failed',
						error: toStructuredError(data.eventData?.error),
						completedAt: now,
						updatedAt: now,
					};

					await upsertThing(
						client,
						Tables.steps,
						`${effectiveRunId}:${step.stepId}`,
						serializeStepRow(step)
					);
				} else if (data.eventType === 'step_retrying' && validatedStep) {
					step = {
						...validatedStep,
						status: 'pending',
						error: toStructuredError(data.eventData?.error),
						retryAfter: data.eventData?.retryAfter,
						updatedAt: now,
					};

					await upsertThing(
						client,
						Tables.steps,
						`${effectiveRunId}:${step.stepId}`,
						serializeStepRow(step)
					);
				} else if (data.eventType === 'hook_created' && 'eventData' in data) {
					const existing = await queryFirstRow<HookRow>(
						client,
						`SELECT * FROM ${Tables.hooks} WHERE tokenHash == crypto::sha256($hookToken) LIMIT 1;`,
						{ hookToken: data.eventData.token }
					);

					if (existing) {
						const conflictEvent = await createHookConflictEvent(
							client,
							effectiveRunId,
							eventId,
							data.correlationId,
							data.eventData.token,
							now
						);

						return {
							event: stripEventDataRefs(conflictEvent, resolveData),
							run,
							step,
							hook: undefined,
						};
					}

					hook = HookSchema.parse({
						runId: effectiveRunId,
						hookId: data.correlationId!,
						token: data.eventData.token,
						metadata: data.eventData.metadata,
						ownerId: '',
						projectId: '',
						environment: '',
						createdAt: now,
						specVersion: SPEC_VERSION_CURRENT,
						isWebhook: (data.eventData as any).isWebhook,
					});

					try {
						await createThing(
							client,
							Tables.hooks,
							hook.hookId,
							serializeHookRow(hook)
						);
					} catch (error) {
						const duplicate = await queryFirstRow<HookRow>(
							client,
							`SELECT * FROM ${Tables.hooks} WHERE tokenHash == crypto::sha256($hookToken) LIMIT 1;`,
							{ hookToken: data.eventData.token }
						);

						if (duplicate) {
							const conflictEvent = await createHookConflictEvent(
								client,
								effectiveRunId,
								eventId,
								data.correlationId,
								data.eventData.token,
								now
							);

							return {
								event: stripEventDataRefs(conflictEvent, resolveData),
								run,
								step,
								hook: undefined,
							};
						}

						const existingHook = await getHook(client, hook.hookId);
						if (existingHook) {
							throw new EntityConflictError(
								`Hook "${data.correlationId}" already exists`
							);
						}

						throw error;
					}
				} else if (data.eventType === 'hook_disposed' && validatedHook) {
					await deleteThing(client, Tables.hooks, validatedHook.hookId);
				} else if (data.eventType === 'wait_created' && 'eventData' in data) {
					const waitId = `${effectiveRunId}-${data.correlationId}`;
					const existing = await getWait(client, waitId);
					if (existing) {
						throw new EntityConflictError(
							`Wait "${data.correlationId}" already exists`
						);
					}

					wait = {
						waitId,
						runId: effectiveRunId,
						status: 'waiting',
						resumeAt: data.eventData.resumeAt,
						completedAt: undefined,
						createdAt: now,
						updatedAt: now,
						specVersion: SPEC_VERSION_CURRENT,
					};

					await createThing(
						client,
						Tables.waits,
						`${effectiveRunId}:${waitId}`,
						serializeWaitRow(wait)
					);
				} else if (data.eventType === 'wait_completed' && data.correlationId) {
					const waitId = `${effectiveRunId}-${data.correlationId}`;
					const existing = await getWait(client, waitId);
					if (!existing) {
						throw new WorkflowWorldError(
							`Wait "${data.correlationId}" not found`
						);
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

					await upsertThing(
						client,
						Tables.waits,
						`${effectiveRunId}:${waitId}`,
						serializeWaitRow(wait)
					);
				}

				await createThing(
					client,
					Tables.events,
					`${effectiveRunId}:${eventId}`,
					serializeEventRow(event)
				);

				return {
					event: stripEventDataRefs(event, resolveData),
					run,
					step,
					hook,
					wait,
				};
			});
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

		async list(params: ListEventsParams): Promise<PaginatedResponse<Event>> {
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
		},

		async listByCorrelationId(params) {
			await ensureSchema();

			const limit = params?.pagination?.limit ?? 100;
			const sortOrder = params.pagination?.sortOrder || 'asc';
			const comparator = sortOrder === 'desc' ? '<' : '>';
			const direction = sortOrder === 'desc' ? 'DESC' : 'ASC';

			const corrConditions: string[] = ['correlationId == $correlationId'];
			const corrVars: Record<string, unknown> = {
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
	};
}
