import type {
	GetChunksOptions,
	StreamChunksResponse,
	Streamer,
	StreamInfoResponse,
} from '@workflow/world';
import { eeq } from 'surrealdb';
import { monotonicFactory } from 'ulid';
import type { SurrealWorldConfig } from './config.js';
import { Tables } from './constants.js';
import {
	queryRows,
	type StreamChunkRow,
	type StreamRow,
	selectThing,
	upsertThing,
} from './records.js';
import type { LiveSubscription, SurrealRpcClient } from './rpc.js';
import { SurrealRpcClient as LiveRpcClient } from './rpc.js';
import type { EnsureSchema } from './storage-helpers.js';
import { Mutex, toBytes } from './util.js';

function encodeCursor(value: Record<string, unknown>): string {
	return encodeURIComponent(JSON.stringify(value));
}

function decodeCursor<T>(value: string | undefined): T | undefined {
	if (!value) {
		return undefined;
	}

	try {
		return JSON.parse(decodeURIComponent(value)) as T;
	} catch {
		return undefined;
	}
}

export type SurrealStreamer = Streamer & {
	close(): Promise<void>;
};

const generateUlid = monotonicFactory();

export function createStreamer(
	client: SurrealRpcClient,
	config: SurrealWorldConfig,
	ensureSchema: EnsureSchema
): SurrealStreamer {
	const locks = new Map<string, Mutex>();
	const activeClosers = new Set<() => Promise<void>>();

	// All concurrent `readFromStream` callers share a single LiveRpcClient
	// instead of opening one WebSocket per reader. Ref-counted so the
	// connection is released after the last reader closes.
	let sharedLiveClient: SurrealRpcClient | null = null;
	let liveClientRefCount = 0;

	async function acquireLiveClient(): Promise<SurrealRpcClient> {
		if (!sharedLiveClient) {
			const lc = new LiveRpcClient(config);
			await lc.connect();
			sharedLiveClient = lc;
		}
		liveClientRefCount++;
		return sharedLiveClient;
	}

	async function releaseLiveClient(): Promise<void> {
		liveClientRefCount--;
		if (liveClientRefCount <= 0 && sharedLiveClient) {
			await sharedLiveClient.close().catch(() => {});
			sharedLiveClient = null;
			liveClientRefCount = 0;
		}
	}

	function getLock(streamId: string): Mutex {
		const existing = locks.get(streamId);
		if (existing) {
			return existing;
		}

		const next = new Mutex();
		locks.set(streamId, next);
		return next;
	}

	async function getStream(streamId: string): Promise<StreamRow | undefined> {
		return selectThing<StreamRow>(client, Tables.streams, streamId);
	}

	async function persistStream(stream: StreamRow) {
		await upsertThing(client, Tables.streams, stream.streamId, stream);
	}

	async function persistChunk(chunk: StreamChunkRow) {
		await upsertThing(
			client,
			Tables.streamChunks,
			`${chunk.streamId}:${chunk.chunkId}`,
			chunk
		);
	}

	async function listChunks(streamId: string): Promise<StreamChunkRow[]> {
		return queryRows<StreamChunkRow>(
			client,
			`
        SELECT * FROM ${Tables.streamChunks}
        WHERE streamId == $streamId
        ORDER BY chunkId ASC;
      `,
			{ streamId }
		);
	}

	async function listChunksAfter(
		streamId: string,
		afterChunkId?: string
	): Promise<StreamChunkRow[]> {
		const conditions: string[] = ['streamId == $streamId'];
		const vars: Record<string, unknown> = { streamId };

		if (afterChunkId) {
			conditions.push('chunkId > $afterChunkId');
			vars.afterChunkId = afterChunkId;
		}

		return queryRows<StreamChunkRow>(
			client,
			`SELECT * FROM ${Tables.streamChunks} WHERE ${conditions.join(' AND ')} ORDER BY chunkId ASC;`,
			vars
		);
	}

	function toUint8Array(chunk: string | Uint8Array | ArrayBuffer): Uint8Array {
		return typeof chunk === 'string'
			? Uint8Array.from(Buffer.from(chunk))
			: toBytes(chunk);
	}

	async function writeChunk(
		name: string,
		runId: string,
		chunkId: `chnk_${string}`,
		chunk: Uint8Array,
		eof: boolean
	) {
		await getLock(name).andThen(async () => {
			const stream = await getStream(name);
			const nextIndex = (stream?.tailIndex ?? -1) + (eof ? 1 : 1);
			const now = new Date();

			await persistChunk({
				streamId: name,
				runId,
				chunkId,
				index: nextIndex,
				data: chunk,
				eof,
				createdAt: now,
			});

			await persistStream({
				streamId: name,
				runId,
				tailIndex: eof ? (stream?.tailIndex ?? -1) : nextIndex,
				done: eof,
				createdAt: stream?.createdAt ?? now,
				updatedAt: now,
				closedAt: eof ? now : stream?.closedAt,
			});
		});
	}

	return {
		async writeToStream(
			name: string,
			_runId: string | Promise<string>,
			chunk: string | Uint8Array
		) {
			await ensureSchema();
			const runId = await _runId;
			await writeChunk(
				name,
				runId,
				`chnk_${generateUlid()}`,
				toUint8Array(chunk),
				false
			);
		},

		async writeToStreamMulti(
			name: string,
			_runId: string | Promise<string>,
			chunks: (string | Uint8Array)[]
		) {
			await ensureSchema();
			if (!chunks.length) {
				return;
			}

			const runId = await _runId;
			if (chunks.length === 1) {
				await writeChunk(
					name,
					runId,
					`chnk_${generateUlid()}`,
					toUint8Array(chunks[0]),
					false
				);
				return;
			}

			await getLock(name).andThen(async () => {
				const stream = await getStream(name);
				let tailIndex = stream?.tailIndex ?? -1;
				const now = new Date();

				for (const chunk of chunks) {
					tailIndex++;
					await persistChunk({
						streamId: name,
						runId,
						chunkId: `chnk_${generateUlid()}`,
						index: tailIndex,
						data: toUint8Array(chunk),
						eof: false,
						createdAt: now,
					});
				}

				await persistStream({
					streamId: name,
					runId,
					tailIndex,
					done: false,
					createdAt: stream?.createdAt ?? now,
					updatedAt: now,
					closedAt: stream?.closedAt,
				});
			});
		},

		async closeStream(name: string, _runId: string | Promise<string>) {
			await ensureSchema();
			const runId = await _runId;
			await writeChunk(
				name,
				runId,
				`chnk_${generateUlid()}`,
				new Uint8Array(),
				true
			);
		},

		async getStreamChunks(
			name: string,
			_runId: string,
			options?: GetChunksOptions
		): Promise<StreamChunksResponse> {
			await ensureSchema();

			const limit = options?.limit ?? 100;
			const cursor = decodeCursor<{ c?: string; i?: number }>(options?.cursor);

			const chunkConditions: string[] = [
				'streamId == $streamId',
				'eof == false',
			];
			const chunkVars: Record<string, unknown> = {
				streamId: name,
				limit: limit + 1,
			};

			if (cursor?.c) {
				chunkConditions.push('chunkId > $cursor');
				chunkVars.cursor = cursor.c;
			}

			const rows = await queryRows<StreamChunkRow>(
				client,
				`SELECT * FROM ${Tables.streamChunks} WHERE ${chunkConditions.join(' AND ')} ORDER BY chunkId ASC LIMIT $limit;`,
				chunkVars
			);

			const stream = await getStream(name);
			const hasMore = rows.length > limit;
			const page = rows.slice(0, limit);
			const baseIndex = cursor?.i ?? 0;

			return {
				data: page.map((row, index) => ({
					index: baseIndex + index,
					data: toBytes(row.data),
				})),
				cursor:
					hasMore && page.length > 0
						? encodeCursor({
								c: page[page.length - 1].chunkId,
								i: baseIndex + page.length,
							})
						: null,
				hasMore,
				done: stream?.done ?? false,
			};
		},

		async getStreamInfo(
			name: string,
			_runId: string
		): Promise<StreamInfoResponse> {
			await ensureSchema();
			const stream = await getStream(name);
			return {
				tailIndex: stream?.tailIndex ?? -1,
				done: stream?.done ?? false,
			};
		},

		async readFromStream(
			name: string,
			startIndex = 0
		): Promise<ReadableStream<Uint8Array>> {
			await ensureSchema();

			const liveClient = await acquireLiveClient();
			let cleaned = false;
			let controllerRef: ReadableStreamDefaultController<Uint8Array> | null =
				null;
			let lastChunkId = '';
			const buffered: StreamChunkRow[] = [];
			const seen = new Set<string>();
			let ready = false;
			let offset = startIndex;
			let reconcileTimer: ReturnType<typeof globalThis.setInterval> | null =
				null;
			let reconciling = false;
			let liveFeed: LiveSubscription | null = null;

			const closer = async () => {
				if (cleaned) {
					return;
				}
				cleaned = true;
				if (reconcileTimer) {
					globalThis.clearInterval(reconcileTimer);
					reconcileTimer = null;
				}
				await liveFeed?.close().catch(() => {});
				await releaseLiveClient();
				activeClosers.delete(closer);
			};
			activeClosers.add(closer);

			const enqueueRow = (row: StreamChunkRow) => {
				if (!controllerRef) {
					buffered.push(row);
					return;
				}

				if (seen.has(row.chunkId) || row.chunkId <= lastChunkId) {
					return;
				}

				seen.add(row.chunkId);
				lastChunkId = row.chunkId;

				if (!row.eof && offset > 0) {
					offset--;
					return;
				}

				if (!row.eof) {
					controllerRef.enqueue(toBytes(row.data));
				}

				if (row.eof) {
					controllerRef.close();
					void closer();
				}
			};

			const reconcile = async () => {
				if (cleaned || reconciling) {
					return;
				}

				reconciling = true;
				try {
					const rows = await listChunksAfter(name, lastChunkId || undefined);
					for (const row of rows) {
						enqueueRow(row);
					}
				} finally {
					reconciling = false;
				}
			};

			liveFeed = await liveClient.liveTable<StreamChunkRow>(
				Tables.streamChunks,
				{ where: eeq('streamId', name) },
				(notification) => {
					if (!ready || !controllerRef) {
						buffered.push(notification.result);
						return;
					}

					enqueueRow(notification.result);
				}
			);

			reconcileTimer = globalThis.setInterval(() => {
				void reconcile().catch(() => {});
			}, 100);

			return new ReadableStream<Uint8Array>({
				async start(controller) {
					controllerRef = controller;

					const rows = await listChunks(name);
					if (offset < 0) {
						const dataCount = rows.filter((row) => !row.eof).length;
						offset = Math.max(0, dataCount + offset);
					}

					for (const row of rows) {
						enqueueRow(row);
					}

					buffered.sort((a, b) => a.chunkId.localeCompare(b.chunkId));
					ready = true;
					for (const row of buffered.splice(0, buffered.length)) {
						enqueueRow(row);
					}
					await reconcile();
				},
				async cancel() {
					await closer();
				},
			});
		},

		async listStreamsByRunId(runId: string): Promise<string[]> {
			await ensureSchema();
			const rows = await queryRows<StreamRow>(
				client,
				`
          SELECT * FROM ${Tables.streams}
          WHERE runId == $runId
          ORDER BY streamId ASC;
        `,
				{ runId }
			);
			return rows.map((row) => row.streamId);
		},

		async close() {
			for (const closer of [...activeClosers]) {
				await closer();
			}
			if (sharedLiveClient) {
				await sharedLiveClient.close().catch(() => {});
				sharedLiveClient = null;
				liveClientRefCount = 0;
			}
		},
	};
}
