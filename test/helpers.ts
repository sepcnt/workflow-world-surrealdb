import {
	GenericContainer,
	type StartedTestContainer,
	Wait,
} from 'testcontainers';
import type { SurrealWorldConfig } from '../src/config.js';
import { Tables } from '../src/constants.js';
import { SurrealRpcClient } from '../src/rpc.js';
import { createSchemaEnsurer } from '../src/schema.js';

const SURREAL_IMAGE = 'surrealdb/surrealdb:v3';
const SURREAL_PORT = 8000;
const TEST_NAMESPACE = 'workflow';
const TEST_DATABASE = 'workflow';
const TEST_USERNAME = 'root';
const TEST_PASSWORD = 'secret';

function sleep(ms: number): Promise<void> {
	return new Promise<void>((resolve) => {
		globalThis.setTimeout(resolve, ms);
	});
}

export type SurrealTestConfig = Required<
	Pick<
		SurrealWorldConfig,
		| 'url'
		| 'namespace'
		| 'database'
		| 'queueLeaseMs'
		| 'queueHeartbeatMs'
		| 'liveReconnectDelayMs'
	>
> & {
	auth: {
		username: string;
		password: string;
	};
};

export type SurrealIntegrationEnv = {
	client: SurrealRpcClient;
	config: SurrealTestConfig;
	container: StartedTestContainer;
	reset(): Promise<void>;
	stop(): Promise<void>;
	applyProcessEnv(): () => void;
};

function toTestConfig(container: StartedTestContainer): SurrealTestConfig {
	return {
		url: `ws://${container.getHost()}:${container.getMappedPort(SURREAL_PORT)}/rpc`,
		namespace: TEST_NAMESPACE,
		database: TEST_DATABASE,
		auth: {
			username: TEST_USERNAME,
			password: TEST_PASSWORD,
		},
		queueLeaseMs: 30_000,
		queueHeartbeatMs: 250,
		liveReconnectDelayMs: 100,
	};
}

async function waitForSurreal(config: SurrealTestConfig): Promise<void> {
	let lastError: unknown;

	for (let attempt = 0; attempt < 40; attempt++) {
		const client = new SurrealRpcClient(config);

		try {
			await client.query('RETURN true;');
			await client.close();
			return;
		} catch (error) {
			lastError = error;
			await client.close().catch(() => {});
			await sleep(250);
		}
	}

	throw lastError instanceof Error
		? lastError
		: new Error('Timed out waiting for SurrealDB to accept RPC requests');
}

export async function clearWorkflowTables(
	client: SurrealRpcClient
): Promise<void> {
	await createSchemaEnsurer(client)();
	await client.query(
		Object.values(Tables)
			.reverse()
			.map((table) => `DELETE ${table};`)
			.join('\n')
	);
}

export async function startSurrealIntegrationEnv(): Promise<SurrealIntegrationEnv> {
	const container = await new GenericContainer(SURREAL_IMAGE)
		.withCommand([
			'start',
			'--user',
			TEST_USERNAME,
			'--pass',
			TEST_PASSWORD,
			'--bind',
			'0.0.0.0:8000',
			'memory',
		])
		.withExposedPorts(SURREAL_PORT)
		// Surreal logs readiness before RPC is fully usable, so we still probe it
		// below. Using the log line is more reliable here than port wait on Docker
		// Desktop for this image.
		.withWaitStrategy(Wait.forLogMessage(/Started web server on .*:8000/))
		.withStartupTimeout(120_000)
		.start();
	const config = toTestConfig(container);
	await waitForSurreal(config);

	const client = new SurrealRpcClient(config);
	await createSchemaEnsurer(client)();

	return {
		client,
		config,
		container,
		async reset() {
			await clearWorkflowTables(client);
		},
		async stop() {
			await client.close().catch(() => {});
			await container.stop().catch(() => {});
		},
		applyProcessEnv() {
			const previous = {
				WORKFLOW_SURREAL_URL: process.env.WORKFLOW_SURREAL_URL,
				WORKFLOW_SURREAL_NAMESPACE: process.env.WORKFLOW_SURREAL_NAMESPACE,
				WORKFLOW_SURREAL_DATABASE: process.env.WORKFLOW_SURREAL_DATABASE,
				WORKFLOW_SURREAL_USERNAME: process.env.WORKFLOW_SURREAL_USERNAME,
				WORKFLOW_SURREAL_PASSWORD: process.env.WORKFLOW_SURREAL_PASSWORD,
			};

			process.env.WORKFLOW_SURREAL_URL = config.url;
			process.env.WORKFLOW_SURREAL_NAMESPACE = config.namespace;
			process.env.WORKFLOW_SURREAL_DATABASE = config.database;
			process.env.WORKFLOW_SURREAL_USERNAME = config.auth.username;
			process.env.WORKFLOW_SURREAL_PASSWORD = config.auth.password;

			return () => {
				for (const [key, value] of Object.entries(previous)) {
					if (value === undefined) {
						delete process.env[key];
					} else {
						process.env[key] = value;
					}
				}
			};
		},
	};
}

export async function collectReadableStream(
	readable: ReadableStream<Uint8Array>
): Promise<Uint8Array[]> {
	const reader = readable.getReader();
	const chunks: Uint8Array[] = [];

	while (true) {
		const { done, value } = await reader.read();
		if (done) {
			break;
		}

		if (value) {
			chunks.push(value);
		}
	}

	return chunks;
}

export function decodeUtf8Chunks(chunks: Uint8Array[]): string[] {
	return chunks.map((chunk) => Buffer.from(chunk).toString('utf8'));
}
