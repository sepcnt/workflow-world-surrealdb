import { once } from 'node:events';
import { createServer, type Server } from 'node:http';
import { JsonTransport } from '@vercel/queue';
import {
	MessageId,
	StepInvokePayloadSchema,
	type ValidQueueName,
} from '@workflow/world';
import { createLocalWorld } from '@workflow/world-local';
import { monotonicFactory } from 'ulid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Tables } from '../src/constants.js';
import { createWorld } from '../src/index.js';
import {
	type QueueJobRow,
	type QueueLaneRow,
	selectThing,
	upsertThing,
} from '../src/records.js';
import {
	type SurrealIntegrationEnv,
	startSurrealIntegrationEnv,
} from './helpers.js';

const generateId = monotonicFactory();

function createDeferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});

	return { promise, resolve, reject };
}

function sleep(ms: number): Promise<void> {
	return new Promise<void>((resolve) => {
		globalThis.setTimeout(resolve, ms);
	});
}

async function withTimeout<T>(
	promise: Promise<T>,
	timeoutMs: number
): Promise<T> {
	let timer: ReturnType<typeof globalThis.setTimeout> | undefined;

	return Promise.race([
		promise.finally(() => {
			if (timer) {
				globalThis.clearTimeout(timer);
			}
		}),
		new Promise<T>((_resolve, reject) => {
			timer = globalThis.setTimeout(() => {
				reject(new Error(`Timed out after ${timeoutMs}ms`));
			}, timeoutMs);
		}),
	]);
}

async function waitForQueueJobCompletion(
	env: SurrealIntegrationEnv,
	messageId: string
): Promise<QueueJobRow | undefined> {
	for (let attempt = 0; attempt < 100; attempt++) {
		const row = await selectThing<QueueJobRow>(
			env.client,
			Tables.queueJobs,
			messageId
		);

		if (!row) {
			return row;
		}

		await sleep(50);
	}

	throw new Error(`Timed out waiting for queue job ${messageId} to complete`);
}

async function startFlowServer(
	handler: (request: Request) => Promise<Response>
): Promise<{ baseUrl: string; close(): Promise<void> }> {
	const server: Server = createServer(async (req, res) => {
		try {
			const chunks: Buffer[] = [];
			for await (const chunk of req) {
				chunks.push(Buffer.from(chunk));
			}

			const url = new URL(req.url ?? '/', 'http://127.0.0.1');
			const headers = new Headers();
			for (const [key, value] of Object.entries(req.headers)) {
				if (Array.isArray(value)) {
					for (const entry of value) {
						headers.append(key, entry);
					}
				} else if (value !== undefined) {
					headers.set(key, value);
				}
			}

			const response = await handler(
				new Request(url, {
					method: req.method,
					headers,
					body: chunks.length > 0 ? Buffer.concat(chunks) : undefined,
				})
			);

			res.statusCode = response.status;
			response.headers.forEach((value, key) => {
				res.setHeader(key, value);
			});
			res.end(Buffer.from(await response.arrayBuffer()));
		} catch (error) {
			res.statusCode = 500;
			res.end(
				error instanceof Error
					? error.message
					: 'Unknown queue test server error'
			);
		}
	});

	server.listen(0, '127.0.0.1');
	await once(server, 'listening');

	const address = server.address();
	if (!address || typeof address === 'string') {
		throw new Error('Unable to resolve test server address');
	}

	return {
		baseUrl: `http://127.0.0.1:${address.port}`,
		close() {
			return new Promise<void>((resolve, reject) => {
				server.close((error) => {
					if (error) {
						reject(error);
					} else {
						resolve();
					}
				});
			});
		},
	};
}

describe('Queue (SurrealDB integration)', () => {
	let env: SurrealIntegrationEnv;

	beforeAll(async () => {
		env = await startSurrealIntegrationEnv();
	}, 120_000);

	beforeEach(async () => {
		await env.reset();
	});

	afterAll(async () => {
		if (env) {
			await env.stop();
		}
	});

	it('starts queue workers without an explicit execution base url when idle', async () => {
		const world = createWorld(env.config);

		await expect(world.start()).resolves.toBeUndefined();
		await world.close?.();
	});

	it('consumes jobs that arrive through LIVE SELECT notifications', async () => {
		const handlerWorld = createLocalWorld();
		const delivered = createDeferred<{
			message: unknown;
			meta: {
				attempt: number;
				messageId: string;
				queueName: ValidQueueName;
			};
		}>();
		const flowHandler = handlerWorld.createQueueHandler(
			'__wkf_workflow_',
			async (message, meta) => {
				delivered.resolve({
					message,
					meta: {
						attempt: meta.attempt,
						messageId: meta.messageId,
						queueName: meta.queueName,
					},
				});
			}
		);
		const server = await startFlowServer(flowHandler);
		const world = createWorld({
			...env.config,
			executionBaseUrl: server.baseUrl,
		});

		try {
			await world.start();

			const queueName = '__wkf_workflow_integration-live';
			const runId = 'wrun_queue_live';
			const now = new Date();
			const transport = new JsonTransport();
			const messageId = MessageId.parse(`msg_${generateId()}`);
			const row: QueueJobRow = {
				messageId,
				version: 1,
				laneKey: `${queueName}:run:${runId}`,
				queueName,
				queuePrefix: '__wkf_workflow_',
				queueId: 'integration-live',
				workflowRunId: runId,
				body: Uint8Array.from(
					Buffer.from(
						transport.serialize({
							runId,
							requestedAt: new Date(now),
						})
					)
				),
				attempt: 1,
				status: 'queued',
				availableAt: now,
				createdAt: now,
				updatedAt: now,
			};

			await upsertThing(env.client, Tables.queueJobs, row.messageId, row);
			await upsertThing(env.client, Tables.queueLanes, row.laneKey, {
				laneKey: row.laneKey,
				queueName,
				queuePrefix: '__wkf_workflow_',
				queueId: 'integration-live',
				queuedCount: 1,
				readyCount: 1,
				processingCount: 0,
				nextAvailableAt: now,
				updatedAt: now,
			});
			await upsertThing(env.client, Tables.queueLaneClaims, row.laneKey, {
				laneKey: row.laneKey,
				updatedAt: now,
			});

			const received = await withTimeout(delivered.promise, 10_000);
			const stored = await waitForQueueJobCompletion(env, row.messageId);

			expect(received.message).toMatchObject({
				runId,
			});
			expect(received.meta).toMatchObject({
				attempt: 1,
				messageId,
				queueName,
			});
			expect(stored).toBeUndefined();
		} finally {
			await server.close();
			await handlerWorld.close?.();
			await world.close?.();
		}
	});

	it('polls delayed jobs until they become claimable', async () => {
		const handlerWorld = createLocalWorld();
		const delivered = createDeferred<{
			receivedAt: number;
			meta: {
				attempt: number;
				messageId: string;
				queueName: ValidQueueName;
			};
		}>();
		const flowHandler = handlerWorld.createQueueHandler(
			'__wkf_workflow_',
			async (_message, meta) => {
				delivered.resolve({
					receivedAt: Date.now(),
					meta: {
						attempt: meta.attempt,
						messageId: meta.messageId,
						queueName: meta.queueName,
					},
				});
			}
		);
		const server = await startFlowServer(flowHandler);
		const world = createWorld({
			...env.config,
			executionBaseUrl: server.baseUrl,
		});

		try {
			await world.start();

			const queueName = '__wkf_workflow_integration-delay';
			const queuedAt = Date.now();
			const queued = await world.queue(
				queueName,
				{
					runId: 'wrun_queue_delay',
				},
				{
					delaySeconds: 1,
				}
			);

			const received = await withTimeout(delivered.promise, 10_000);
			const stored = await waitForQueueJobCompletion(
				env,
				MessageId.parse(queued.messageId!)
			);

			expect(received.receivedAt - queuedAt).toBeGreaterThanOrEqual(750);
			expect(received.meta).toMatchObject({
				attempt: 1,
				messageId: queued.messageId,
				queueName,
			});
			expect(stored).toBeUndefined();
		} finally {
			await server.close();
			await handlerWorld.close?.();
			await world.close?.();
		}
	});

	it('keeps lane summary stable for idempotent delayed enqueue', async () => {
		const world = createWorld({
			...env.config,
			executionBaseUrl: 'http://127.0.0.1:3000',
		});

		try {
			const first = await world.queue(
				'__wkf_step_idempotent-lane',
				{
					workflowName: 'idempotent-lane',
					workflowRunId: 'wrun_idempotent_lane',
					workflowStartedAt: Date.now(),
					stepId: 'step_idempotent_lane',
				},
				{
					delaySeconds: 60,
					idempotencyKey: 'stable-step-idempotency-key',
				}
			);
			const second = await world.queue(
				'__wkf_step_idempotent-lane',
				{
					workflowName: 'idempotent-lane',
					workflowRunId: 'wrun_idempotent_lane',
					workflowStartedAt: Date.now(),
					stepId: 'step_idempotent_lane',
				},
				{
					delaySeconds: 60,
					idempotencyKey: 'stable-step-idempotency-key',
				}
			);

			const job = await selectThing<QueueJobRow>(
				env.client,
				Tables.queueJobs,
				MessageId.parse(first.messageId!)
			);

			expect(second.messageId).toBe(first.messageId);
			expect(job).toBeDefined();

			const laneStatements = await env.client.query<QueueLaneRow[]>(
				`SELECT * FROM ${Tables.queueLanes} WHERE laneKey == $laneKey LIMIT 1;`,
				{
					laneKey: job!.laneKey,
				}
			);
			const lane = laneStatements.at(-1)?.[0];

			expect(lane).toMatchObject({
				laneKey: job!.laneKey,
				queuedCount: 1,
				readyCount: 0,
				processingCount: 0,
			});
		} finally {
			await world.close?.();
		}
	});

	it('avoids duplicate delivery when multiple workers compete for the same jobs', async () => {
		const handlerWorld = createLocalWorld();
		const seen = new Map<string, number>();
		const delivered = createDeferred<void>();
		const expectedJobs = 12;
		const stepHandler = handlerWorld.createQueueHandler(
			'__wkf_step_',
			async (message) => {
				const payload = StepInvokePayloadSchema.parse(message);
				seen.set(payload.stepId, (seen.get(payload.stepId) || 0) + 1);
				if (seen.size === expectedJobs) {
					delivered.resolve();
				}
			}
		);
		const server = await startFlowServer(async (request) => {
			if (request.url.endsWith('/.well-known/workflow/v1/step')) {
				return stepHandler(request);
			}

			return Response.json({ error: 'Not found' }, { status: 404 });
		});
		const workerA = createWorld({
			...env.config,
			executionBaseUrl: server.baseUrl,
			queueConcurrency: 1,
			queueHeartbeatMs: 250,
		});
		const workerB = createWorld({
			...env.config,
			executionBaseUrl: server.baseUrl,
			queueConcurrency: 1,
			queueHeartbeatMs: 250,
		});

		try {
			await Promise.all([workerA.start(), workerB.start()]);

			const queued = await Promise.all(
				Array.from({ length: expectedJobs }, async (_, index) => {
					const message = await workerA.queue('__wkf_step_multi-worker', {
						workflowName: 'multi-worker',
						workflowRunId: `wrun_multi_${index}`,
						workflowStartedAt: Date.now(),
						stepId: `step_multi_${index}`,
					});

					return MessageId.parse(message.messageId!);
				})
			);

			await withTimeout(delivered.promise, 10_000);
			await Promise.all(
				queued.map((messageId) => waitForQueueJobCompletion(env, messageId))
			);
			await sleep(500);

			expect(seen.size).toBe(expectedJobs);
			expect([...seen.values()]).toEqual(Array(expectedJobs).fill(1));
		} finally {
			await server.close();
			await handlerWorld.close?.();
			await Promise.all([workerA.close?.(), workerB.close?.()]);
		}
	});

	it('does not duplicate successful deliveries when retries race across workers', async () => {
		const handlerWorld = createLocalWorld();
		const seen = new Map<string, number>();
		const failedAttempts = new Map<string, number>();
		const delivered = createDeferred<void>();
		const expectedJobs = 60;
		const stepHandler = handlerWorld.createQueueHandler(
			'__wkf_step_',
			async (message, meta) => {
				const payload = StepInvokePayloadSchema.parse(message);
				const stepIndex = Number.parseInt(
					payload.stepId.slice(payload.stepId.lastIndexOf('_') + 1),
					10
				);

				if (stepIndex % 5 === 0 && meta.attempt === 1) {
					failedAttempts.set(
						payload.stepId,
						(failedAttempts.get(payload.stepId) || 0) + 1
					);
					throw new Error(`Benchmark induced failure for ${payload.stepId}`);
				}

				seen.set(payload.stepId, (seen.get(payload.stepId) || 0) + 1);
				if (seen.size === expectedJobs) {
					delivered.resolve();
				}
			}
		);
		const server = await startFlowServer(async (request) => {
			if (request.url.endsWith('/.well-known/workflow/v1/step')) {
				return stepHandler(request);
			}

			return Response.json({ error: 'Not found' }, { status: 404 });
		});
		const workers = Array.from({ length: 4 }, () =>
			createWorld({
				...env.config,
				executionBaseUrl: server.baseUrl,
				queueConcurrency: 2,
				queueHeartbeatMs: 250,
				suppressBenchmarkErrors: true,
			})
		);

		try {
			await Promise.all(workers.map((worker) => worker.start()));

			const queued = await Promise.all(
				Array.from({ length: expectedJobs }, async (_, index) => {
					const message = await workers[0].queue('__wkf_step_retry-race', {
						workflowName: 'retry-race',
						workflowRunId: `wrun_retry_${index}`,
						workflowStartedAt: Date.now(),
						stepId: `step_retry_${index}`,
					});

					return MessageId.parse(message.messageId!);
				})
			);

			await withTimeout(delivered.promise, 30_000);
			await Promise.all(
				queued.map((messageId) => waitForQueueJobCompletion(env, messageId))
			);
			await sleep(1_000);

			expect(failedAttempts.size).toBe(expectedJobs / 5);
			expect([...failedAttempts.values()]).toEqual(
				Array(expectedJobs / 5).fill(1)
			);
			expect(seen.size).toBe(expectedJobs);
			expect([...seen.values()]).toEqual(Array(expectedJobs).fill(1));
		} finally {
			await server.close();
			await handlerWorld.close?.();
			await Promise.all(workers.map((worker) => worker.close?.()));
		}
	}, 60_000);
});
