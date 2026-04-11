import type { ExprLike } from 'surrealdb';
import { BoundQuery, type RecordId, Surreal, Table } from 'surrealdb';
import type { SurrealWorldConfig } from './config.js';
import { resolveConfig, resolveRpcUrl } from './config.js';
import { coerceErrorMessage } from './util.js';

export interface LiveNotification<T = unknown> {
	action: 'CREATE' | 'UPDATE' | 'DELETE';
	result: T;
}

export interface LiveSubscription {
	close(): Promise<void>;
}

/**
 * Thin adapter around the official `surrealdb` SDK.
 *
 * Keeps the rest of the package on CBOR-over-WebSocket while exposing a small,
 * explicit surface that mirrors the SDK instead of re-implementing a JSON-RPC
 * protocol on top of it.
 */
export class SurrealRpcClient {
	private db = new Surreal();
	private connectPromise: Promise<void> | null = null;
	private closeRequested = false;
	private closeListeners = new Set<() => void>();
	private liveSubscriptions = new Set<LiveSubscription>();

	constructor(private readonly rawConfig: SurrealWorldConfig = {}) {}

	private get config() {
		return resolveConfig(this.rawConfig);
	}

	async connect(): Promise<void> {
		if (this.db.status === 'connected') {
			return;
		}
		if (this.connectPromise) {
			return this.connectPromise;
		}

		this.closeRequested = false;
		this.connectPromise = (async () => {
			const url = resolveRpcUrl(this.config.url);
			const { auth, namespace, database } = this.config;

			this.db.subscribe('disconnected', () => {
				if (!this.closeRequested) {
					for (const callback of this.closeListeners) {
						callback();
					}
				}
			});

			const connectOptions: Record<string, unknown> = {
				versionCheck: false,
			};
			if (namespace) {
				connectOptions.namespace = namespace;
			}
			if (database) {
				connectOptions.database = database;
			}
			if (auth && 'username' in auth) {
				connectOptions.authentication = {
					username: auth.username,
					password: auth.password,
				};
			}

			await this.db.connect(new URL(url), connectOptions as any);
			if (auth?.token) {
				await this.db.authenticate(auth.token);
			}
		})();

		try {
			await this.connectPromise;
		} catch (error) {
			this.connectPromise = null;
			throw error;
		}
	}

	async reconnect(): Promise<void> {
		await this.close();
		this.db = new Surreal();
		await this.connect();
	}

	async select<T = unknown>(thing: RecordId): Promise<T> {
		await this.connect();
		return (await this.db.select<T>(thing)) as T;
	}

	async create<T = unknown>(thing: RecordId, value: unknown): Promise<T> {
		await this.connect();
		return (await this.db
			.create<T>(thing)
			.content(value as any)
			.output('after')) as T;
	}

	async upsert<T = unknown>(thing: RecordId, value: unknown): Promise<T> {
		await this.connect();
		return (await this.db
			.upsert<T>(thing)
			.content(value as any)
			.output('after')) as T;
	}

	async delete<T = unknown>(thing: RecordId): Promise<T> {
		await this.connect();
		return (await this.db.delete<T>(thing).output('before')) as T;
	}

	async query<T = unknown>(
		sql: string | BoundQuery,
		vars?: Record<string, unknown>
	): Promise<T[]> {
		await this.connect();

		const responses =
			sql instanceof BoundQuery
				? await this.db.query(sql).responses()
				: await this.db.query(sql, vars).responses();

		const results: T[] = [];
		for (const response of responses) {
			if (!response.success) {
				throw new Error(
					coerceErrorMessage((response as any).error?.message ?? response)
				);
			}

			results.push(response.result as T);
		}

		return results;
	}

	async run<T = unknown>(name: string, args: unknown[] = []): Promise<T> {
		const bindings: Record<string, unknown> = {};
		const argRefs = args.map((arg, index) => {
			if (arg === undefined) {
				return 'NONE';
			}

			const key = `arg${index}`;
			bindings[key] = arg;
			return `$${key}`;
		});

		const [result] = await this.query<T>(
			new BoundQuery(
				`RETURN ${name}(${argRefs.join(', ')});`,
				Object.keys(bindings).length > 0 ? bindings : undefined
			)
		);

		return result as T;
	}

	async liveTable<T = unknown>(
		table: string,
		options: {
			fields?: string[];
			where?: ExprLike;
		} = {},
		onMessage: (notification: LiveNotification<T>) => void
	): Promise<LiveSubscription> {
		await this.connect();

		let liveQuery = this.db.live<T>(new Table(table));
		if (options.fields && options.fields.length > 0) {
			liveQuery = liveQuery.fields(...(options.fields as any));
		}
		if (options.where) {
			liveQuery = liveQuery.where(options.where);
		}

		const liveSub = await liveQuery;
		let closed = false;
		const unsubscribe = liveSub.subscribe((message: any) => {
			const action = message.action as string;
			if (action === 'KILLED') {
				return;
			}

			onMessage({
				action: action as LiveNotification<T>['action'],
				result: message.value as T,
			});
		});

		const subscription: LiveSubscription = {
			close: async () => {
				if (closed) {
					return;
				}

				closed = true;
				unsubscribe();
				this.liveSubscriptions.delete(subscription);
				await liveSub.kill().catch(() => {});
			},
		};

		this.liveSubscriptions.add(subscription);
		return subscription;
	}

	onClose(callback: () => void): () => void {
		this.closeListeners.add(callback);
		return () => {
			this.closeListeners.delete(callback);
		};
	}

	async close(): Promise<void> {
		this.closeRequested = true;

		for (const subscription of [...this.liveSubscriptions]) {
			await subscription.close().catch(() => {});
		}

		this.connectPromise = null;
		try {
			await this.db.close();
		} catch {}
	}
}
