import { WorkflowWorldError } from '@workflow/errors';
import type { Storage } from '@workflow/world';
import { DEFAULT_RESOLVE_DATA_OPTION, Tables } from './constants.js';
import { filterStepData } from './filters.js';
import type { StepRow } from './records.js';
import { deserializeStepRow, queryFirstRow, queryRows } from './records.js';
import type { SurrealRpcClient } from './rpc.js';
import type { EnsureSchema } from './storage-helpers.js';
import { getStep } from './storage-helpers.js';

export function createStepsStorage(
	client: SurrealRpcClient,
	ensureSchema: EnsureSchema
): Storage['steps'] {
	return {
		get: (async (runId: string | undefined, stepId: string, params?: any) => {
			await ensureSchema();

			const row = runId
				? undefined
				: await queryFirstRow<StepRow>(
						client,
						`SELECT * FROM ${Tables.steps} WHERE stepId == $stepId LIMIT 1;`,
						{ stepId }
					);
			const step = row
				? deserializeStepRow(row)
				: runId
					? await getStep(client, runId, stepId)
					: undefined;

			if (!step) {
				throw new WorkflowWorldError(`Step not found: ${stepId}`);
			}

			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;
			return filterStepData(step, resolveData);
		}) as Storage['steps']['get'],

		list: (async (params: any) => {
			await ensureSchema();

			const limit = params?.pagination?.limit ?? 20;

			const conditions: string[] = ['runId == $runId'];
			const vars: Record<string, unknown> = {
				runId: params.runId,
				limit: limit + 1,
			};

			if (params?.pagination?.cursor) {
				conditions.push('stepId < $cursor');
				vars.cursor = params.pagination.cursor;
			}

			const where = `WHERE ${conditions.join(' AND ')}`;

			const rows = await queryRows<StepRow>(
				client,
				`SELECT * FROM ${Tables.steps} ${where} ORDER BY stepId DESC LIMIT $limit;`,
				vars
			);

			const hasMore = rows.length > limit;
			const values = rows.slice(0, limit).map(deserializeStepRow);
			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;

			return {
				data: values.map((step) => filterStepData(step, resolveData)),
				hasMore,
				cursor: values.at(-1)?.stepId ?? null,
			};
		}) as Storage['steps']['list'],
	};
}
