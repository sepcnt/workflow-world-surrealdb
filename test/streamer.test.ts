import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createSchemaEnsurer } from '../src/schema.js';
import { createStreamer } from '../src/streamer.js';
import {
	collectReadableStream,
	decodeUtf8Chunks,
	type SurrealIntegrationEnv,
	startSurrealIntegrationEnv,
} from './helpers.js';

describe('Streamer (SurrealDB integration)', () => {
	let env: SurrealIntegrationEnv;
	let streamer: ReturnType<typeof createStreamer>;

	beforeAll(async () => {
		env = await startSurrealIntegrationEnv();
		streamer = createStreamer(
			env.client,
			env.config,
			createSchemaEnsurer(env.client)
		);
	}, 120_000);

	beforeEach(async () => {
		await env.reset();
	});

	afterAll(async () => {
		if (streamer) {
			await streamer.close();
		}

		if (env) {
			await env.stop();
		}
	});

	it('replays stored chunks and continues with LIVE updates until EOF', async () => {
		const runId = 'wrun_stream_live';

		await streamer.writeToStream('stream-live', runId, 'alpha');

		const readable = await streamer.readFromStream('stream-live', 0);
		const readAll = collectReadableStream(readable);

		await streamer.writeToStream('stream-live', runId, 'beta');
		await streamer.writeToStreamMulti?.('stream-live', runId, [
			'gamma',
			'delta',
		]);
		await streamer.closeStream('stream-live', runId);

		expect(decodeUtf8Chunks(await readAll)).toEqual([
			'alpha',
			'beta',
			'gamma',
			'delta',
		]);

		const page1 = await streamer.getStreamChunks('stream-live', runId, {
			limit: 2,
		});
		const page2 = await streamer.getStreamChunks('stream-live', runId, {
			limit: 2,
			cursor: page1.cursor ?? undefined,
		});
		const info = await streamer.getStreamInfo('stream-live', runId);

		expect(decodeUtf8Chunks(page1.data.map((chunk) => chunk.data))).toEqual([
			'alpha',
			'beta',
		]);
		expect(decodeUtf8Chunks(page2.data.map((chunk) => chunk.data))).toEqual([
			'gamma',
			'delta',
		]);
		expect(page2.done).toBe(true);
		expect(info).toEqual({
			tailIndex: 3,
			done: true,
		});
	});
});
