import { WorkflowRunNotFoundError } from '@workflow/errors';
import type { Storage } from '@workflow/world';
import { DEFAULT_RESOLVE_DATA_OPTION, Tables } from './constants.js';
import { filterRunData } from './filters.js';
import type { RunRow } from './records.js';
import { deserializeRunStored, queryRows } from './records.js';
import type { SurrealRpcClient } from './rpc.js';
import type { EnsureSchema } from './storage-helpers.js';
import { getRun } from './storage-helpers.js';

export function createRunsStorage(
	client: SurrealRpcClient,
	ensureSchema: EnsureSchema
): Storage['runs'] {
	return {
		get: (async (id: string, params?: any) => {
			await ensureSchema();

			const run = await getRun(client, id);
			if (!run) {
				throw new WorkflowRunNotFoundError(id);
			}

			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;
			return filterRunData(run, resolveData);
		}) as Storage['runs']['get'],

		list: (async (params?: any) => {
			await ensureSchema();

			const limit = params?.pagination?.limit ?? 20;

			const conditions: string[] = [];
			const vars: Record<string, unknown> = { limit: limit + 1 };

			if (params?.workflowName) {
				conditions.push('workflowName == $workflowName');
				vars.workflowName = params.workflowName;
			}
			if (params?.status) {
				conditions.push('status == $status');
				vars.status = params.status;
			}
			if (params?.pagination?.cursor) {
				conditions.push('runId < $cursor');
				vars.cursor = params.pagination.cursor;
			}

			const where =
				conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

			const rows = await queryRows<RunRow>(
				client,
				`SELECT * FROM ${Tables.runs} ${where} ORDER BY runId DESC LIMIT $limit;`,
				vars
			);

			const hasMore = rows.length > limit;
			const values = rows.slice(0, limit).map(deserializeRunStored);
			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;

			return {
				data: values.map((run) => filterRunData(run, resolveData)),
				hasMore,
				cursor: values.at(-1)?.runId ?? null,
			};
		}) as Storage['runs']['list'],
	};
}
