import type { Queue, Storage, ValidQueueName, World } from '@workflow/world';
import type { SurrealWorldConfig } from './config.js';
import { resolveConfig } from './config.js';
import { createQueue } from './queue.js';
import { SurrealRpcClient } from './rpc.js';
import { createSchemaEnsurer } from './schema.js';
import {
	createEventsStorage,
	createHooksStorage,
	createRunsStorage,
	createStepsStorage,
} from './storage.js';
import { createStreamer } from './streamer.js';

async function reenqueueActiveRuns(
	runs: Storage['runs'],
	enqueue: Queue['queue'],
	label: string
): Promise<void> {
	let reenqueued = 0;
	for (const status of ['pending', 'running'] as const) {
		let cursor: string | undefined;
		let hasMore = true;
		while (hasMore) {
			const page = await runs.list({
				status,
				resolveData: 'none',
				pagination: { cursor },
			});
			for (const run of page.data) {
				try {
					const queueName: ValidQueueName = `__wkf_workflow_${run.workflowName}`;
					await enqueue(queueName, { runId: run.runId });
					reenqueued++;
				} catch (err) {
					console.warn(
						`[${label}] Failed to re-enqueue run ${run.runId}: ${err}`
					);
				}
			}
			hasMore = page.hasMore;
			cursor = page.cursor ?? undefined;
		}
	}
	if (reenqueued > 0) {
		console.log(
			`[${label}] Re-enqueued ${reenqueued} active run(s) on startup`
		);
	}
}

function createStorage(
	client: SurrealRpcClient,
	ensureSchema: () => Promise<void>
): Storage {
	return {
		runs: createRunsStorage(client, ensureSchema),
		events: createEventsStorage(client, ensureSchema),
		hooks: createHooksStorage(client, ensureSchema),
		steps: createStepsStorage(client, ensureSchema),
	};
}

export function createWorld(
	config: SurrealWorldConfig = {}
): World & { start(): Promise<void> } {
	const resolved = resolveConfig(config);
	const client = new SurrealRpcClient(resolved);
	const ensureSchema = createSchemaEnsurer(client);
	const storage = createStorage(client, ensureSchema);
	const queue = createQueue(resolved, client, ensureSchema);
	const streamer = createStreamer(client, resolved, ensureSchema);

	return {
		...storage,
		...queue,
		...streamer,
		async start() {
			await ensureSchema();
			await queue.start();
			await reenqueueActiveRuns(storage.runs, queue.queue, 'world-surreal');
		},
		async close() {
			await streamer.close();
			await queue.close();
			await client.close();
		},
	};
}

export default createWorld;

export type { SurrealWorldConfig } from './config.js';
export { SurrealRpcClient } from './rpc.js';
export { createSchemaEnsurer } from './schema.js';
