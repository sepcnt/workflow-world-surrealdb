import { getWorkflowPort } from '@workflow/utils/get-port';

export type SurrealAuthConfig =
	| {
			token: string;
			username?: never;
			password?: never;
			access?: never;
	  }
	| {
			username: string;
			password: string;
			token?: never;
			access?: string;
	  };

export type SurrealWorldConfig = {
	url?: string;
	namespace?: string;
	database?: string;
	auth?: SurrealAuthConfig;
	executionBaseUrl?: string;
	port?: number;
	queueConcurrency?: number;
	queueLaneShards?: number;
	stepQueueLaneShards?: number;
	workflowQueueLaneShards?: number;
	queueLeaseMs?: number;
	queueHeartbeatMs?: number;
	liveReconnectDelayMs?: number;
	suppressBenchmarkErrors?: boolean;
};

export type ResolvedSurrealWorldConfig = Required<
	Pick<
		SurrealWorldConfig,
		| 'url'
		| 'namespace'
		| 'database'
		| 'queueConcurrency'
		| 'stepQueueLaneShards'
		| 'workflowQueueLaneShards'
		| 'queueLeaseMs'
		| 'queueHeartbeatMs'
		| 'liveReconnectDelayMs'
		| 'suppressBenchmarkErrors'
	>
> &
	Pick<SurrealWorldConfig, 'auth' | 'executionBaseUrl' | 'port'>;

function resolveOptionalString(
	value: string | undefined,
	envName: string
): string | undefined {
	return value ?? process.env[envName] ?? undefined;
}

function resolvePositiveInt(
	value: number | undefined,
	envNames: string[],
	fallback: number
): number {
	if (Number.isFinite(value) && (value as number) > 0) {
		return value as number;
	}

	for (const envName of envNames) {
		const parsed = Number.parseInt(process.env[envName] || '', 10);
		if (Number.isFinite(parsed) && parsed > 0) {
			return parsed;
		}
	}

	return fallback;
}

function resolveBoolean(
	value: boolean | undefined,
	envName: string,
	fallback = false
): boolean {
	if (value !== undefined) {
		return value;
	}

	const raw = process.env[envName];
	if (raw === undefined) {
		return fallback;
	}

	return raw === '1' || raw.toLowerCase() === 'true';
}

export function resolveConfig(
	config: SurrealWorldConfig = {}
): ResolvedSurrealWorldConfig {
	const queueLaneShards = resolvePositiveInt(
		config.queueLaneShards,
		['WORKFLOW_SURREAL_QUEUE_LANE_SHARDS'],
		16
	);
	const stepQueueLaneShards = resolvePositiveInt(
		config.stepQueueLaneShards,
		['WORKFLOW_SURREAL_STEP_QUEUE_LANE_SHARDS'],
		queueLaneShards
	);

	return {
		url:
			resolveOptionalString(config.url, 'WORKFLOW_SURREAL_URL') ||
			'ws://127.0.0.1:8000/rpc',
		namespace:
			resolveOptionalString(config.namespace, 'WORKFLOW_SURREAL_NAMESPACE') ||
			'workflow',
		database:
			resolveOptionalString(config.database, 'WORKFLOW_SURREAL_DATABASE') ||
			'workflow',
		executionBaseUrl:
			config.executionBaseUrl ?? process.env.WORKFLOW_LOCAL_BASE_URL,
		port:
			Number.isFinite(config.port) && (config.port as number) > 0
				? (config.port as number)
				: undefined,
		auth:
			config.auth ||
			(process.env.WORKFLOW_SURREAL_TOKEN
				? {
						token: process.env.WORKFLOW_SURREAL_TOKEN,
					}
				: process.env.WORKFLOW_SURREAL_USERNAME &&
						process.env.WORKFLOW_SURREAL_PASSWORD
					? {
							username: process.env.WORKFLOW_SURREAL_USERNAME,
							password: process.env.WORKFLOW_SURREAL_PASSWORD,
							...(process.env.WORKFLOW_SURREAL_ACCESS
								? { access: process.env.WORKFLOW_SURREAL_ACCESS }
								: {}),
						}
					: undefined),
		queueConcurrency: resolvePositiveInt(
			config.queueConcurrency,
			['WORKFLOW_SURREAL_QUEUE_CONCURRENCY'],
			10
		),
		stepQueueLaneShards,
		workflowQueueLaneShards: resolvePositiveInt(
			config.workflowQueueLaneShards,
			['WORKFLOW_SURREAL_WORKFLOW_QUEUE_LANE_SHARDS'],
			stepQueueLaneShards
		),
		queueLeaseMs: resolvePositiveInt(
			config.queueLeaseMs,
			['WORKFLOW_SURREAL_QUEUE_LEASE_MS'],
			300000
		),
		queueHeartbeatMs: resolvePositiveInt(
			config.queueHeartbeatMs,
			['WORKFLOW_SURREAL_QUEUE_HEARTBEAT_MS'],
			10000
		),
		liveReconnectDelayMs: resolvePositiveInt(
			config.liveReconnectDelayMs,
			['WORKFLOW_SURREAL_LIVE_RECONNECT_DELAY_MS'],
			1000
		),
		suppressBenchmarkErrors: resolveBoolean(
			config.suppressBenchmarkErrors,
			'WORKFLOW_WORLD_BENCH_SILENT_ERRORS'
		),
	};
}

export async function resolveExecutionBaseUrl(
	config: Pick<ResolvedSurrealWorldConfig, 'executionBaseUrl' | 'port'>
): Promise<string> {
	if (config.executionBaseUrl) {
		return config.executionBaseUrl;
	}

	if (process.env.WORKFLOW_LOCAL_BASE_URL) {
		return process.env.WORKFLOW_LOCAL_BASE_URL;
	}

	if (typeof config.port === 'number') {
		return `http://localhost:${config.port}`;
	}

	if (process.env.PORT) {
		return `http://localhost:${process.env.PORT}`;
	}

	const detectedPort = await getWorkflowPort();
	if (detectedPort) {
		return `http://localhost:${detectedPort}`;
	}

	throw new Error('Unable to resolve base URL for workflow queue.');
}

export function resolveRpcUrl(url: string): string {
	const parsed = new URL(url);

	if (parsed.protocol === 'http:') {
		parsed.protocol = 'ws:';
	} else if (parsed.protocol === 'https:') {
		parsed.protocol = 'wss:';
	}

	if (!parsed.pathname || parsed.pathname === '/') {
		parsed.pathname = '/rpc';
	} else if (!parsed.pathname.endsWith('/rpc')) {
		parsed.pathname = `${parsed.pathname.replace(/\/$/, '')}/rpc`;
	}

	return parsed.toString();
}
