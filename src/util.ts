export class Mutex {
	promise: Promise<unknown> = Promise.resolve();

	andThen<T>(fn: () => Promise<T> | T): Promise<T> {
		this.promise = this.promise.then(
			() => fn(),
			() => fn()
		);
		return this.promise as Promise<T>;
	}
}

export function toBytes(value: ArrayBuffer | Uint8Array): Uint8Array {
	return value instanceof Uint8Array ? value : new Uint8Array(value);
}

export function isRetryableSurrealWriteConflict(error: unknown): boolean {
	const message = coerceErrorMessage(error);
	return /transaction conflict|write conflict|retry the transaction/i.test(
		message
	);
}

export async function retrySurrealWrite<T>(
	operation: () => Promise<T>,
	options?: {
		attempts?: number;
		minDelayMs?: number;
		maxDelayMs?: number;
	}
): Promise<T> {
	const attempts = options?.attempts ?? 6;
	const minDelayMs = options?.minDelayMs ?? 5;
	const maxDelayMs = options?.maxDelayMs ?? 100;

	let lastError: unknown;

	for (let attempt = 0; attempt < attempts; attempt++) {
		try {
			return await operation();
		} catch (error) {
			lastError = error;
			if (!isRetryableSurrealWriteConflict(error) || attempt === attempts - 1) {
				throw error;
			}

			await new Promise<void>((resolve) => {
				globalThis.setTimeout(
					resolve,
					Math.min(minDelayMs * 2 ** attempt, maxDelayMs)
				);
			});
		}
	}

	throw lastError;
}

export function coerceErrorMessage(error: unknown): string {
	if (error instanceof Error) {
		return error.message;
	}

	return typeof error === 'string' ? error : JSON.stringify(error);
}
