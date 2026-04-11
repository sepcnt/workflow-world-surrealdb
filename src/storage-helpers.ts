import type { Hook, Step, Wait, WorkflowRun } from '@workflow/world';
import { Tables } from './constants.js';
import {
	deserializeHookRow,
	deserializeRunStored,
	deserializeStepRow,
	deserializeWaitRow,
	type HookRow,
	type RunRow,
	type StepRow,
	selectThing,
	type WaitRow,
} from './records.js';
import type { SurrealRpcClient } from './rpc.js';
import { Mutex } from './util.js';

export type EnsureSchema = () => Promise<void>;

export function createLockMap() {
	const locks = new Map<string, Mutex>();

	return (key: string): Mutex => {
		const existing = locks.get(key);
		if (existing) {
			return existing;
		}

		const next = new Mutex();
		locks.set(key, next);
		return next;
	};
}

export async function deleteAllHooksForRun(
	client: SurrealRpcClient,
	runId: string
): Promise<void> {
	await client.query(`DELETE FROM ${Tables.hooks} WHERE runId == $runId;`, {
		runId,
	});
}

export async function deleteAllWaitsForRun(
	client: SurrealRpcClient,
	runId: string
): Promise<void> {
	await client.query(`DELETE FROM ${Tables.waits} WHERE runId == $runId;`, {
		runId,
	});
}

export async function getRun(
	client: SurrealRpcClient,
	runId: string
): Promise<WorkflowRun | undefined> {
	const row = await selectThing<RunRow>(client, Tables.runs, runId);
	return row ? deserializeRunStored(row) : undefined;
}

export async function getStep(
	client: SurrealRpcClient,
	runId: string,
	stepId: string
): Promise<Step | undefined> {
	const row = await selectThing<StepRow>(
		client,
		Tables.steps,
		`${runId}:${stepId}`
	);
	return row ? deserializeStepRow(row) : undefined;
}

export async function getHook(
	client: SurrealRpcClient,
	hookId: string
): Promise<Hook | undefined> {
	const row = await selectThing<HookRow>(client, Tables.hooks, hookId);
	return row ? deserializeHookRow(row) : undefined;
}

export async function getWait(
	client: SurrealRpcClient,
	waitId: string
): Promise<Wait | undefined> {
	const row = await selectThing<WaitRow>(client, Tables.waits, waitId);
	return row ? deserializeWaitRow(row) : undefined;
}
