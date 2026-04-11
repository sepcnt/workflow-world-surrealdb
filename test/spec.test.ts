import { createTestSuite } from '@workflow/world-testing';
import { afterAll, beforeAll, test } from 'vitest';
import {
	type SurrealIntegrationEnv,
	startSurrealIntegrationEnv,
} from './helpers.js';

let env: SurrealIntegrationEnv;

beforeAll(async () => {
	env = await startSurrealIntegrationEnv();
	env.applyProcessEnv();
}, 120_000);

afterAll(async () => {
	if (env) {
		await env.stop();
	}
});

test('smoke', () => {});
createTestSuite('./dist/index.js');
