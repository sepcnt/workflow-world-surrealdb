import type { Event, Hook, Step, Wait, WorkflowRun } from '@workflow/world';
import {
	EventSchema,
	HookSchema,
	StepSchema,
	WaitSchema,
	WorkflowRunSchema,
} from '@workflow/world';
import { RecordId } from 'surrealdb';
import type { SurrealRpcClient } from './rpc.js';
import { toBytes } from './util.js';

function unwrapOne<T>(value: T | T[] | null | undefined): T | undefined {
	return Array.isArray(value) ? value[0] : (value ?? undefined);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
	if (!value || typeof value !== 'object') {
		return false;
	}

	const prototype = Object.getPrototypeOf(value);
	return prototype === Object.prototype || prototype === null;
}

function normalizeBinaryValue(
	value: ArrayBuffer | Uint8Array | undefined
): Uint8Array | undefined {
	return value === undefined ? undefined : toBytes(value);
}

function normalizeBinaryTree<T>(value: T): T {
	if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
		return toBytes(value) as T;
	}

	if (Array.isArray(value)) {
		let changed = false;
		const next = value.map((entry) => {
			const normalized = normalizeBinaryTree(entry);
			changed ||= normalized !== entry;
			return normalized;
		});

		return (changed ? next : value) as T;
	}

	if (isPlainObject(value)) {
		let changed = false;
		const next: Record<string, unknown> = {};

		for (const [key, entry] of Object.entries(value)) {
			const normalized = normalizeBinaryTree(entry);
			next[key] = normalized;
			changed ||= normalized !== entry;
		}

		return (changed ? next : value) as T;
	}

	return value;
}

export interface RunRow {
	runId: string;
	deploymentId: string;
	status: WorkflowRun['status'];
	workflowName: string;
	specVersion?: number;
	executionContext?: Record<string, unknown>;
	input: ArrayBuffer | Uint8Array;
	output?: ArrayBuffer | Uint8Array;
	error?: WorkflowRun['error'];
	createdAt: Date;
	updatedAt: Date;
	completedAt?: Date;
	startedAt?: Date;
	expiredAt?: Date;
}

export interface EventRow {
	runId: string;
	eventId: string;
	eventType: Event['eventType'];
	correlationId?: string;
	eventData?: Record<string, unknown>;
	createdAt: Date;
	specVersion?: number;
}

export interface StepRow {
	runId: string;
	stepId: string;
	stepName: string;
	status: Step['status'];
	input: ArrayBuffer | Uint8Array;
	output?: ArrayBuffer | Uint8Array;
	error?: Step['error'];
	attempt: number;
	startedAt?: Date;
	completedAt?: Date;
	createdAt: Date;
	updatedAt: Date;
	retryAfter?: Date;
	specVersion?: number;
}

export interface HookRow {
	runId: string;
	hookId: string;
	token: string;
	tokenHash?: string;
	metadata?: ArrayBuffer | Uint8Array;
	ownerId: string;
	projectId: string;
	environment: string;
	createdAt: Date;
	specVersion?: number;
	isWebhook?: boolean;
}

export interface WaitRow {
	waitId: string;
	runId: string;
	status: Wait['status'];
	resumeAt?: Date;
	completedAt?: Date;
	createdAt: Date;
	updatedAt: Date;
	specVersion?: number;
}

export interface StreamRow {
	streamId: string;
	runId: string;
	tailIndex: number;
	done: boolean;
	createdAt: Date;
	updatedAt: Date;
	closedAt?: Date;
}

export interface StreamChunkRow {
	streamId: string;
	runId: string;
	chunkId: `chnk_${string}`;
	index: number;
	data: ArrayBuffer | Uint8Array;
	eof: boolean;
	createdAt: Date;
}

export interface QueueJobRow {
	messageId: string;
	version: number;
	laneKey: string;
	queueName: string;
	queuePrefix: '__wkf_step_' | '__wkf_workflow_';
	queueId: string;
	workflowRunId?: string;
	body: ArrayBuffer | Uint8Array;
	headers?: Record<string, string>;
	idempotencyKey?: string;
	attempt: number;
	status: 'queued' | 'processing';
	availableAt: Date;
	leaseUntil?: Date;
	lockedBy?: string;
	createdAt: Date;
	updatedAt: Date;
	completedAt?: Date;
}

export type ClaimedQueueJobRow = Pick<
	QueueJobRow,
	| 'messageId'
	| 'queueName'
	| 'queuePrefix'
	| 'workflowRunId'
	| 'body'
	| 'headers'
	| 'attempt'
>;

export interface QueueLaneRow {
	laneKey: string;
	queueName: string;
	queuePrefix: '__wkf_step_' | '__wkf_workflow_';
	queueId: string;
	queuedCount: number;
	readyCount: number;
	processingCount: number;
	nextAvailableAt?: Date;
	updatedAt: Date;
}

export interface QueueLaneClaimRow {
	laneKey: string;
	updatedAt: Date;
	drainingBy?: string;
	drainingUntil?: Date;
}

export function deserializeRunStored(row: RunRow): WorkflowRun {
	return WorkflowRunSchema.parse({
		...row,
		executionContext: normalizeBinaryTree(row.executionContext),
		input: toBytes(row.input),
		output: normalizeBinaryValue(row.output),
	});
}

export function serializeRunStored(run: WorkflowRun): RunRow {
	return {
		runId: run.runId,
		deploymentId: run.deploymentId,
		status: run.status,
		workflowName: run.workflowName,
		specVersion: run.specVersion,
		executionContext: run.executionContext,
		input: run.input as Uint8Array,
		output: run.output as Uint8Array | undefined,
		error: run.error,
		createdAt: run.createdAt,
		updatedAt: run.updatedAt,
		completedAt: run.completedAt,
		startedAt: run.startedAt,
		expiredAt: run.expiredAt,
	};
}

export function deserializeEventRow(row: EventRow): Event {
	return EventSchema.parse({
		...row,
		eventData: normalizeBinaryTree(row.eventData),
	});
}

export function serializeEventRow(event: Event): EventRow {
	return {
		runId: event.runId,
		eventId: event.eventId,
		eventType: event.eventType,
		correlationId: event.correlationId,
		eventData: 'eventData' in event ? event.eventData : undefined,
		createdAt: event.createdAt,
		specVersion: event.specVersion,
	};
}

export function deserializeStepRow(row: StepRow): Step {
	return StepSchema.parse({
		...row,
		input: toBytes(row.input),
		output: normalizeBinaryValue(row.output),
	});
}

export function serializeStepRow(step: Step): StepRow {
	return {
		runId: step.runId,
		stepId: step.stepId,
		stepName: step.stepName,
		status: step.status,
		input: step.input as Uint8Array,
		output: step.output as Uint8Array | undefined,
		error: step.error,
		attempt: step.attempt,
		startedAt: step.startedAt,
		completedAt: step.completedAt,
		createdAt: step.createdAt,
		updatedAt: step.updatedAt,
		retryAfter: step.retryAfter,
		specVersion: step.specVersion,
	};
}

export function deserializeHookRow(row: HookRow): Hook {
	return HookSchema.parse({
		...row,
		metadata: normalizeBinaryValue(row.metadata),
	});
}

export function serializeHookRow(hook: Hook): HookRow {
	return {
		runId: hook.runId,
		hookId: hook.hookId,
		token: hook.token,
		metadata: hook.metadata as Uint8Array | undefined,
		ownerId: hook.ownerId,
		projectId: hook.projectId,
		environment: hook.environment,
		createdAt: hook.createdAt,
		specVersion: hook.specVersion,
		isWebhook: hook.isWebhook ?? true,
	};
}

export function deserializeWaitRow(row: WaitRow): Wait {
	return WaitSchema.parse(row);
}

export function serializeWaitRow(wait: Wait): WaitRow {
	return {
		waitId: wait.waitId,
		runId: wait.runId,
		status: wait.status,
		resumeAt: wait.resumeAt,
		completedAt: wait.completedAt,
		createdAt: wait.createdAt,
		updatedAt: wait.updatedAt,
		specVersion: wait.specVersion,
	};
}

export async function selectThing<T>(
	client: SurrealRpcClient,
	table: string,
	key: string
): Promise<T | undefined> {
	const result = await client.select<T | T[] | null>(new RecordId(table, key));
	return unwrapOne(result);
}

export async function createThing<T>(
	client: SurrealRpcClient,
	table: string,
	key: string,
	value: T
): Promise<T | undefined> {
	const result = await client.create<T | T[] | null>(
		new RecordId(table, key),
		value
	);
	return unwrapOne(result);
}

export async function upsertThing<T>(
	client: SurrealRpcClient,
	table: string,
	key: string,
	value: T
): Promise<T | undefined> {
	const result = await client.upsert<T | T[] | null>(
		new RecordId(table, key),
		value
	);
	return unwrapOne(result);
}

export async function deleteThing<T>(
	client: SurrealRpcClient,
	table: string,
	key: string
): Promise<T | undefined> {
	const result = await client.delete<T | T[] | null>(new RecordId(table, key));
	return unwrapOne(result);
}

export async function queryRows<T>(
	client: SurrealRpcClient,
	sql: string,
	vars?: Record<string, unknown>
): Promise<T[]> {
	const result = await client.query<T[]>(sql, vars);
	return result[0] || [];
}

export async function queryFirstRow<T>(
	client: SurrealRpcClient,
	sql: string,
	vars?: Record<string, unknown>
): Promise<T | undefined> {
	const rows = await queryRows<T>(client, sql, vars);
	return rows[0];
}
