import { config } from 'dotenv';
import type { SurrealWorldConfig } from './config.js';
import { SurrealRpcClient } from './rpc.js';
import { createSchemaEnsurer } from './schema.js';

const LOCAL_HOSTNAMES = new Set(['127.0.0.1', 'localhost', '::1', '[::1]']);

function isLocalhostUrl(raw: string | undefined): boolean {
	if (!raw) return true;
	try {
		return LOCAL_HOSTNAMES.has(new URL(raw).hostname);
	} catch {
		return false;
	}
}

export async function setupDatabase(): Promise<void> {
	config();

	const rawConfig: SurrealWorldConfig = {};

	// When no auth is present in the environment and the target is a
	// local SurrealDB, fall back to the community world's documented
	// `root/root` default. This lets `workflow-surreal-setup` run in CI
	// pipelines that expose WORKFLOW_SURREAL_* env vars in a step that
	// executes after the setup command itself.
	const hasAuthEnv =
		Boolean(process.env.WORKFLOW_SURREAL_TOKEN) ||
		Boolean(
			process.env.WORKFLOW_SURREAL_USERNAME &&
				process.env.WORKFLOW_SURREAL_PASSWORD
		);
	const targetsLocal = isLocalhostUrl(process.env.WORKFLOW_SURREAL_URL);

	if (!hasAuthEnv && targetsLocal) {
		console.log(
			'workflow-surreal-setup: no auth env vars detected; falling back to local default (root/root).'
		);
		rawConfig.auth = { username: 'root', password: 'root' };
	}

	const client = new SurrealRpcClient(rawConfig);

	try {
		await createSchemaEnsurer(client)();
		console.log('SurrealDB workflow schema created successfully.');
		await client.close();
	} catch (error) {
		await client.close().catch(() => {});
		console.error('Failed to create SurrealDB workflow schema:', error);
		process.exitCode = 1;
	}
}

if (import.meta.url === `file://${process.argv[1]}`) {
	void setupDatabase();
}
