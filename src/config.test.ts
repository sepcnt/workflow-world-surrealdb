import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
	resolveConfig,
	resolveExecutionBaseUrl,
	resolveRpcUrl,
} from './config.js';

vi.mock('@workflow/utils/get-port', () => ({
	getWorkflowPort: vi.fn(),
}));

const ENV_KEYS = [
	'WORKFLOW_LOCAL_BASE_URL',
	'WORKFLOW_SURREAL_QUEUE_CONCURRENCY',
	'WORKFLOW_SURREAL_QUEUE_LANE_SHARDS',
	'WORKFLOW_SURREAL_STEP_QUEUE_LANE_SHARDS',
	'WORKFLOW_SURREAL_WORKFLOW_QUEUE_LANE_SHARDS',
	'WORKFLOW_WORLD_BENCH_SILENT_ERRORS',
	'PORT',
] as const;

describe('config resolution', () => {
	let previousEnv: Record<(typeof ENV_KEYS)[number], string | undefined>;

	beforeEach(() => {
		previousEnv = Object.fromEntries(
			ENV_KEYS.map((key) => [key, process.env[key]])
		) as Record<(typeof ENV_KEYS)[number], string | undefined>;

		for (const key of ENV_KEYS) {
			delete process.env[key];
		}
	});

	afterEach(() => {
		for (const key of ENV_KEYS) {
			const value = previousEnv[key];
			if (value === undefined) {
				delete process.env[key];
			} else {
				process.env[key] = value;
			}
		}

		vi.clearAllMocks();
	});

	it('hydrates queue settings from env and lets explicit config win', () => {
		process.env.WORKFLOW_LOCAL_BASE_URL = 'http://127.0.0.1:4010';
		process.env.WORKFLOW_SURREAL_QUEUE_CONCURRENCY = '12';
		process.env.WORKFLOW_SURREAL_QUEUE_LANE_SHARDS = '24';
		process.env.WORKFLOW_SURREAL_WORKFLOW_QUEUE_LANE_SHARDS = '48';
		process.env.WORKFLOW_WORLD_BENCH_SILENT_ERRORS = 'true';

		expect(resolveConfig()).toMatchObject({
			executionBaseUrl: 'http://127.0.0.1:4010',
			queueConcurrency: 12,
			stepQueueLaneShards: 24,
			workflowQueueLaneShards: 48,
			suppressBenchmarkErrors: true,
		});

		expect(
			resolveConfig({
				executionBaseUrl: 'http://127.0.0.1:4030',
				queueConcurrency: 6,
				stepQueueLaneShards: 8,
				workflowQueueLaneShards: 10,
				suppressBenchmarkErrors: false,
			})
		).toMatchObject({
			executionBaseUrl: 'http://127.0.0.1:4030',
			queueConcurrency: 6,
			stepQueueLaneShards: 8,
			workflowQueueLaneShards: 10,
			suppressBenchmarkErrors: false,
		});
	});

	it('resolves execution base url with the expected precedence chain', async () => {
		const { getWorkflowPort } = await import('@workflow/utils/get-port');
		vi.mocked(getWorkflowPort).mockResolvedValue(5173);
		process.env.WORKFLOW_LOCAL_BASE_URL = 'http://127.0.0.1:4010';
		process.env.PORT = '4020';

		await expect(
			resolveExecutionBaseUrl({
				executionBaseUrl: 'http://127.0.0.1:4030',
				port: 4040,
			})
		).resolves.toBe('http://127.0.0.1:4030');

		await expect(
			resolveExecutionBaseUrl({
				executionBaseUrl: undefined,
				port: 4040,
			})
		).resolves.toBe('http://127.0.0.1:4010');

		delete process.env.WORKFLOW_LOCAL_BASE_URL;
		await expect(
			resolveExecutionBaseUrl({
				executionBaseUrl: undefined,
				port: 4040,
			})
		).resolves.toBe('http://localhost:4040');

		await expect(
			resolveExecutionBaseUrl({
				executionBaseUrl: undefined,
				port: undefined,
			})
		).resolves.toBe('http://localhost:4020');
		expect(getWorkflowPort).not.toHaveBeenCalled();

		delete process.env.PORT;
		await expect(
			resolveExecutionBaseUrl({
				executionBaseUrl: undefined,
				port: undefined,
			})
		).resolves.toBe('http://localhost:5173');
		expect(getWorkflowPort).toHaveBeenCalledTimes(1);
	});

	it('throws when no execution base url can be inferred', async () => {
		const { getWorkflowPort } = await import('@workflow/utils/get-port');
		vi.mocked(getWorkflowPort).mockResolvedValue(undefined);

		await expect(
			resolveExecutionBaseUrl({
				executionBaseUrl: undefined,
				port: undefined,
			})
		).rejects.toThrow('Unable to resolve base URL for workflow queue.');
	});

	it('normalizes rpc urls across protocol and path variants', () => {
		expect(
			[
				['http://127.0.0.1:8000', 'ws://127.0.0.1:8000/rpc'],
				['ws://127.0.0.1:8000/rpc', 'ws://127.0.0.1:8000/rpc'],
				['https://example.com/surreal', 'wss://example.com/surreal/rpc'],
			].map(([input]) => resolveRpcUrl(input))
		).toEqual([
			'ws://127.0.0.1:8000/rpc',
			'ws://127.0.0.1:8000/rpc',
			'wss://example.com/surreal/rpc',
		]);
	});
});
