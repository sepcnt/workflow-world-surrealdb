import { HookNotFoundError } from '@workflow/errors';
import type {
	ListHooksParams,
	PaginatedResponse,
	Storage,
} from '@workflow/world';
import { DEFAULT_RESOLVE_DATA_OPTION, Tables } from './constants.js';
import { filterHookData } from './filters.js';
import type { HookRow } from './records.js';
import { deserializeHookRow, queryFirstRow, queryRows } from './records.js';
import type { SurrealRpcClient } from './rpc.js';
import type { EnsureSchema } from './storage-helpers.js';
import { getHook } from './storage-helpers.js';

export function createHooksStorage(
	client: SurrealRpcClient,
	ensureSchema: EnsureSchema
): Storage['hooks'] {
	return {
		async get(hookId, params) {
			await ensureSchema();

			const hook = await getHook(client, hookId);
			if (!hook) {
				throw new HookNotFoundError(hookId);
			}

			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;
			return filterHookData(hook, resolveData);
		},

		async getByToken(token, params) {
			await ensureSchema();

			const row = await queryFirstRow<HookRow>(
				client,
				`SELECT * FROM ${Tables.hooks} WHERE tokenHash == crypto::sha256($hookToken) LIMIT 1;`,
				{ hookToken: token }
			);
			if (!row) {
				throw new HookNotFoundError(token);
			}

			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;
			return filterHookData(deserializeHookRow(row), resolveData);
		},

		async list(params: ListHooksParams): Promise<PaginatedResponse<any>> {
			await ensureSchema();

			const limit = params?.pagination?.limit ?? 100;
			const sortOrder = params?.pagination?.sortOrder ?? 'asc';
			const comparator = sortOrder === 'asc' ? '>' : '<';
			const direction = sortOrder === 'asc' ? 'ASC' : 'DESC';

			const conditions: string[] = [];
			const vars: Record<string, unknown> = { limit: limit + 1 };

			if (params?.runId) {
				conditions.push('runId == $runId');
				vars.runId = params.runId;
			}
			if (params?.pagination?.cursor) {
				conditions.push(`hookId ${comparator} $cursor`);
				vars.cursor = params.pagination.cursor;
			}

			const where =
				conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

			const rows = await queryRows<HookRow>(
				client,
				`SELECT * FROM ${Tables.hooks} ${where} ORDER BY hookId ${direction} LIMIT $limit;`,
				vars
			);

			const hasMore = rows.length > limit;
			const values = rows.slice(0, limit).map(deserializeHookRow);
			const resolveData = params?.resolveData ?? DEFAULT_RESOLVE_DATA_OPTION;

			return {
				data: values.map((hook) => filterHookData(hook, resolveData)),
				hasMore,
				cursor: values.at(-1)?.hookId ?? null,
			};
		},
	};
}
