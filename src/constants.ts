export const Tables = {
	runs: 'workflow_runs',
	events: 'workflow_events',
	steps: 'workflow_steps',
	hooks: 'workflow_hooks',
	waits: 'workflow_waits',
	streams: 'workflow_streams',
	streamChunks: 'workflow_stream_chunks',
	queueJobs: 'workflow_queue_jobs',
	queueLanes: 'workflow_queue_lanes',
	queueLaneClaims: 'workflow_queue_lane_claims',
	queueSignals: 'workflow_queue_signals',
} as const;

export const DEFAULT_CHANGEFEED_RETENTION = '3d';
export const DEFAULT_RESOLVE_DATA_OPTION = 'all';
