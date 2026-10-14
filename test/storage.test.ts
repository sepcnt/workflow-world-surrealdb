import { EntityConflictError, RunExpiredError } from '@workflow/errors';
import { eventIdToSlot, slotToEventId } from '@workflow/world';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { SurrealRpcClient } from '../src/rpc.js';
import {
	createEventsStorage,
	createHooksStorage,
	createRunsStorage,
	createStepsStorage,
} from '../src/storage.js';
import {
	type SurrealIntegrationEnv,
	startSurrealIntegrationEnv,
} from './helpers.js';

describe('Storage (SurrealDB integration)', () => {
	let env: SurrealIntegrationEnv;
	let runs: ReturnType<typeof createRunsStorage>;
	let steps: ReturnType<typeof createStepsStorage>;
	let hooks: ReturnType<typeof createHooksStorage>;
	let events: ReturnType<typeof createEventsStorage>;

	beforeAll(async () => {
		env = await startSurrealIntegrationEnv();
		const ensureSchema = async () => {};
		runs = createRunsStorage(env.client, ensureSchema);
		steps = createStepsStorage(env.client, ensureSchema);
		hooks = createHooksStorage(env.client, ensureSchema);
		events = createEventsStorage(env.client, ensureSchema);
	}, 120_000);

	beforeEach(async () => {
		await env.reset();
	});

	afterAll(async () => {
		if (env) {
			await env.stop();
		}
	});

	type RawRunRow = {
		executionContext?: Record<string, unknown>;
		input?: ArrayBuffer;
	};

	type RawStepRow = {
		input?: ArrayBuffer;
	};

	type RawHookRow = {
		metadata?: ArrayBuffer;
	};

	type RawEventRow = {
		eventData?: Record<string, unknown>;
	};

	it('materializes runs, steps, hooks, and events through the event log', async () => {
		const created = await events.create(null, {
			eventType: 'run_created',
			eventData: {
				deploymentId: 'dep-surreal',
				workflowName: 'integration-workflow',
				input: new Uint8Array([1, 2, 3]),
				executionContext: { region: 'test' },
			},
		});

		expect(created.run).toMatchObject({
			status: 'pending',
			workflowName: 'integration-workflow',
			deploymentId: 'dep-surreal',
			executionContext: { region: 'test' },
			input: new Uint8Array([1, 2, 3]),
		});

		const runId = created.run!.runId;

		const started = await events.create(runId, {
			eventType: 'run_started',
		});
		expect(started.run?.status).toBe('running');

		const stepCreated = await events.create(runId, {
			eventType: 'step_created',
			correlationId: 'step_live_1',
			eventData: {
				stepName: 'first-step',
				input: new Uint8Array([9]),
			},
		});
		expect(stepCreated.step).toMatchObject({
			runId,
			stepId: 'step_live_1',
			status: 'pending',
			stepName: 'first-step',
			input: new Uint8Array([9]),
		});

		const hookCreated = await events.create(runId, {
			eventType: 'hook_created',
			correlationId: 'hook_live_1',
			eventData: {
				token: 'hook-token-live',
				metadata: new Uint8Array([4, 5, 6]),
			},
		});
		expect(hookCreated.hook).toMatchObject({
			runId,
			hookId: 'hook_live_1',
			token: 'hook-token-live',
		});
		expect(hookCreated.hook?.metadata).toEqual(new Uint8Array([4, 5, 6]));

		const run = await runs.get(runId);
		const step = await steps.get(runId, 'step_live_1');
		const hook = await hooks.getByToken('hook-token-live');
		const listedEvents = await events.list({
			runId,
			pagination: { sortOrder: 'asc' },
		});

		expect(run.status).toBe('running');
		expect(step.status).toBe('pending');
		expect(hook.hookId).toBe('hook_live_1');
		expect(hook.metadata).toEqual(new Uint8Array([4, 5, 6]));
		expect(listedEvents.data.map((event) => event.eventType)).toEqual([
			'run_created',
			'run_started',
			'step_created',
			'hook_created',
		]);

		const [runRows, stepRows, hookRows, runEventRows, hookEventRows] =
			await Promise.all([
				env.client.query<RawRunRow[]>(
					'SELECT executionContext, input FROM workflow_runs WHERE runId == $runId LIMIT 1;',
					{ runId }
				),
				env.client.query<RawStepRow[]>(
					'SELECT input FROM workflow_steps WHERE runId == $runId AND stepId == $stepId LIMIT 1;',
					{ runId, stepId: 'step_live_1' }
				),
				env.client.query<RawHookRow[]>(
					'SELECT metadata FROM workflow_hooks WHERE hookId == $hookId LIMIT 1;',
					{ hookId: 'hook_live_1' }
				),
				env.client.query<RawEventRow[]>(
					'SELECT eventData FROM workflow_events WHERE runId == $runId AND eventType == $eventType LIMIT 1;',
					{ runId, eventType: 'run_created' }
				),
				env.client.query<RawEventRow[]>(
					'SELECT eventData FROM workflow_events WHERE runId == $runId AND eventType == $eventType LIMIT 1;',
					{ runId, eventType: 'hook_created' }
				),
			]);

		expect(runRows[0]?.[0]?.executionContext).toEqual({ region: 'test' });
		expect(runRows[0]?.[0]?.input).toBeInstanceOf(ArrayBuffer);
		expect(stepRows[0]?.[0]?.input).toBeInstanceOf(ArrayBuffer);
		expect(hookRows[0]?.[0]?.metadata).toBeInstanceOf(ArrayBuffer);
		expect(runEventRows[0]?.[0]?.eventData).toMatchObject({
			executionContext: { region: 'test' },
		});
		expect(runEventRows[0]?.[0]?.eventData?.input).toBeInstanceOf(ArrayBuffer);
		expect(hookEventRows[0]?.[0]?.eventData).toMatchObject({
			token: 'hook-token-live',
		});
		expect(hookEventRows[0]?.[0]?.eventData?.metadata).toBeInstanceOf(
			ArrayBuffer
		);
	});

	it('releases hooks when a run reaches a terminal state', async () => {
		const created = await events.create(null, {
			eventType: 'run_created',
			eventData: {
				deploymentId: 'dep-surreal',
				workflowName: 'cleanup-workflow',
				input: new Uint8Array(),
			},
		});
		const runId = created.run!.runId;

		await events.create(runId, {
			eventType: 'hook_created',
			correlationId: 'hook_cleanup_1',
			eventData: {
				token: 'hook-token-cleanup',
			},
		});

		expect((await hooks.list({ runId })).data).toHaveLength(1);

		const completed = await events.create(runId, {
			eventType: 'run_completed',
			eventData: {
				output: new Uint8Array([42]),
			},
		});

		expect(completed.run?.status).toBe('completed');
		expect((await hooks.list({ runId })).data).toHaveLength(0);
	});

	it('emits hook_conflict when the same token is reused across runs', async () => {
		const firstRun = await events.create(null, {
			eventType: 'run_created',
			eventData: {
				deploymentId: 'dep-surreal',
				workflowName: 'hook-owner-a',
				input: new Uint8Array(),
			},
		});
		const secondRun = await events.create(null, {
			eventType: 'run_created',
			eventData: {
				deploymentId: 'dep-surreal',
				workflowName: 'hook-owner-b',
				input: new Uint8Array(),
			},
		});

		await events.create(firstRun.run!.runId, {
			eventType: 'hook_created',
			correlationId: 'hook_owner_a',
			eventData: {
				token: 'shared-hook-token',
			},
		});

		const conflict = await events.create(secondRun.run!.runId, {
			eventType: 'hook_created',
			correlationId: 'hook_owner_b',
			eventData: {
				token: 'shared-hook-token',
			},
		});

		expect(conflict.event?.eventType).toBe('hook_conflict');
		expect(conflict.hook).toBeUndefined();
		expect((await hooks.list({})).data).toHaveLength(1);
	});

	async function createRun(workflowName = 'slot-workflow') {
		const created = await events.create(null, {
			eventType: 'run_created',
			eventData: {
				deploymentId: 'dep-surreal',
				workflowName,
				input: new Uint8Array(),
			},
		});
		return created.run!.runId;
	}

	it('numbers events densely by slot from 1', async () => {
		const runId = await createRun();
		await events.create(runId, { eventType: 'run_started' });
		await events.create(runId, {
			eventType: 'step_created',
			correlationId: 'step_slot_1',
			eventData: { stepName: 'slot', input: new Uint8Array() },
		});

		const listed = await events.list({ runId });
		expect(listed.data.map((event) => event.eventId)).toEqual([
			slotToEventId(1),
			slotToEventId(2),
			slotToEventId(3),
		]);
	});

	it('answers a repeated run_started without journaling an event', async () => {
		const runId = await createRun();
		const first = await events.create(runId, { eventType: 'run_started' });
		const second = await events.create(runId, { eventType: 'run_started' });

		expect(first.event?.eventType).toBe('run_started');
		expect(second.event).toBeUndefined();
		expect(second.run?.status).toBe('running');
		expect((await events.list({ runId })).data).toHaveLength(2);
	});

	it('keeps slots dense and reports skipped events when writers in separate processes race', async () => {
		const runId = await createRun();
		await events.create(runId, { eventType: 'run_started' });

		// Separate clients and storage instances share nothing in process, so
		// only the store can settle the race, as between two deployments.
		const writers = await Promise.all(
			Array.from({ length: 4 }, async () => {
				const client = new SurrealRpcClient(env.config);
				return {
					client,
					events: createEventsStorage(client, async () => {}),
				};
			})
		);
		const width = 24;
		try {
			const results = await Promise.all(
				Array.from({ length: width }, (_, index) =>
					writers[index % writers.length].events.create(
						runId,
						{
							eventType: 'step_created',
							correlationId: `step_race_${index}`,
							eventData: { stepName: 'race', input: new Uint8Array() },
						},
						// Every writer replayed the same two-event log.
						{ eventCount: 2 }
					)
				)
			);

			const listed = await events.list({
				runId,
				pagination: { limit: 1000 },
			});
			expect(listed.data.map((event) => eventIdToSlot(event.eventId))).toEqual(
				Array.from({ length: width + 2 }, (_, index) => index + 1)
			);

			for (const result of results) {
				const committed = eventIdToSlot(result.event!.eventId)!;
				const reported = (result.events ?? []).map((event) =>
					eventIdToSlot(event.eventId)
				);
				expect(reported).toEqual(
					Array.from({ length: committed - 3 }, (_, index) => index + 3)
				);
				expect(result.hasMore ?? false).toBe(false);
			}
		} finally {
			await Promise.all(writers.map((writer) => writer.client.close()));
		}
	}, 60_000);

	it('refuses step_started on a terminal run even while the step reads running', async () => {
		const runId = await createRun();
		await events.create(runId, { eventType: 'run_started' });
		await events.create(runId, {
			eventType: 'step_created',
			correlationId: 'step_terminal',
			eventData: { stepName: 'terminal', input: new Uint8Array() },
		});
		await events.create(runId, {
			eventType: 'step_started',
			correlationId: 'step_terminal',
		});
		await events.create(runId, { eventType: 'run_cancelled' });

		expect((await steps.get(runId, 'step_terminal')).status).toBe('running');
		await expect(
			events.create(runId, {
				eventType: 'step_started',
				correlationId: 'step_terminal',
			})
		).rejects.toSatisfy((error) => RunExpiredError.is(error));
		// The step already in flight may still record its outcome.
		const completed = await events.create(runId, {
			eventType: 'step_completed',
			correlationId: 'step_terminal',
			eventData: { result: new Uint8Array([1]) },
		});
		expect(completed.step?.status).toBe('completed');
	});

	it('creates the step on a lazy step_started exactly once', async () => {
		const runId = await createRun();
		await events.create(runId, { eventType: 'run_started' });
		const lazyStart = {
			eventType: 'step_started' as const,
			correlationId: 'step_lazy',
			eventData: { stepName: 'lazy', input: new Uint8Array([7]) },
		};

		const started = await events.create(runId, lazyStart);
		expect(started.stepCreated).toBe(true);
		expect(started.step).toMatchObject({ status: 'running', attempt: 1 });
		expect(started.step?.input).toEqual(new Uint8Array([7]));
		await expect(events.create(runId, lazyStart)).rejects.toSatisfy((error) =>
			EntityConflictError.is(error)
		);

		const listed = await events.list({ runId });
		expect(listed.data.map((event) => event.eventType)).toEqual([
			'run_created',
			'run_started',
			'step_created',
			'step_started',
		]);
	});

	it('filters runs by any of several statuses', async () => {
		const pendingRun = await createRun('status-a');
		const runningRun = await createRun('status-b');
		await events.create(runningRun, { eventType: 'run_started' });
		const cancelledRun = await createRun('status-c');
		await events.create(cancelledRun, { eventType: 'run_cancelled' });

		const listed = await runs.list({ status: ['pending', 'cancelled'] });
		expect(listed.data.map((run) => run.runId).sort()).toEqual(
			[pendingRun, cancelledRun].sort()
		);
		expect((await runs.list({ status: [] })).data).toEqual([]);
	});
});
