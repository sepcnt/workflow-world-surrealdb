import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
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
});
