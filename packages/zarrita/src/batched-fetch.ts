import type { AbsolutePath, AsyncReadable, RangeQuery } from "@zarrita/storage";

interface PendingRequest {
	offset: number;
	length: number;
	resolve: (value: Uint8Array | undefined) => void;
	reject: (reason: Error) => void;
}

interface RangeGroup {
	offset: number;
	length: number;
	requests: PendingRequest[];
}

export interface RangeBatchingOptions {
	signal?: AbortSignal;
	gapThreshold?: number;
}

export interface RangeBatchingStats {
	batchedRequests: number;
	mergedRequests: number;
}

const DEFAULT_GAP_THRESHOLD = 32768;

/**
 * Groups sorted requests into contiguous ranges, merging across small gaps.
 */
function groupRequests(
	sorted: PendingRequest[],
	gapThreshold: number,
): RangeGroup[] {
	if (sorted.length === 0) {
		return [];
	}
	let groups: RangeGroup[] = [];
	let current = [sorted[0]];
	let groupStart = sorted[0].offset;
	let groupEnd = sorted[0].offset + sorted[0].length;

	for (let i = 1; i < sorted.length; i++) {
		let req = sorted[i];
		let reqEnd = req.offset + req.length;
		if (req.offset <= groupEnd + gapThreshold) {
			current.push(req);
			groupEnd = Math.max(groupEnd, reqEnd);
		} else {
			groups.push({
				offset: groupStart,
				length: groupEnd - groupStart,
				requests: current,
			});
			current = [req];
			groupStart = req.offset;
			groupEnd = reqEnd;
		}
	}
	groups.push({
		offset: groupStart,
		length: groupEnd - groupStart,
		requests: current,
	});
	return groups;
}

/**
 * A store wrapper that batches concurrent `getRange()` calls within a single
 * microtask, merges adjacent byte ranges across small gaps, and dispatches
 * merged fetches to the inner store.
 *
 * Inspired by geotiff.js
 * {@link https://github.com/geotiffjs/geotiff.js/blob/master/src/source/blockedsource.js BlockedSource}.
 *
 * Suffix requests (shard index reads) bypass batching and are deduplicated
 * in-flight.
 *
 * @see {@link withRangeBatching}
 */
export class BatchedRangeStore implements AsyncReadable<RequestInit> {
	#inner: AsyncReadable<RequestInit>;
	#innerGetRange: NonNullable<AsyncReadable<RequestInit>["getRange"]>;
	#pending = new Map<AbsolutePath, PendingRequest[]>();
	#scheduled = false;
	#signal: AbortSignal | undefined;
	#gapThreshold: number;
	#inflight = new Map<string, Promise<Uint8Array | undefined>>();
	#stats: RangeBatchingStats = { batchedRequests: 0, mergedRequests: 0 };

	get stats(): Readonly<RangeBatchingStats> {
		return { ...this.#stats };
	}

	constructor(
		inner: AsyncReadable<RequestInit>,
		options?: RangeBatchingOptions,
	) {
		if (!inner.getRange) {
			throw new Error("BatchedRangeStore requires a store with getRange");
		}
		this.#inner = inner;
		this.#innerGetRange = inner.getRange.bind(inner);
		this.#signal = options?.signal;
		this.#gapThreshold = options?.gapThreshold ?? DEFAULT_GAP_THRESHOLD;
	}

	get(
		key: AbsolutePath,
		options?: RequestInit,
	): Promise<Uint8Array | undefined> {
		return this.#inner.get(key, options);
	}

	getRange(
		key: AbsolutePath,
		range: RangeQuery,
	): Promise<Uint8Array | undefined> {
		if ("suffixLength" in range) {
			return this.#getSuffix(key, range.suffixLength);
		}

		let { offset, length } = range;
		this.#stats.batchedRequests++;

		return new Promise((resolve, reject) => {
			let pending = this.#pending.get(key);
			if (!pending) {
				pending = [];
				this.#pending.set(key, pending);
			}
			pending.push({ offset, length, resolve, reject });

			if (!this.#scheduled) {
				this.#scheduled = true;
				queueMicrotask(() => this.#flush());
			}
		});
	}

	#getSuffix(
		key: AbsolutePath,
		suffixLength: number,
	): Promise<Uint8Array | undefined> {
		let cacheKey = `${key}:suffix:${suffixLength}`;
		let inflight = this.#inflight.get(cacheKey);
		if (inflight) {
			return inflight;
		}

		let opts = this.#signal ? { signal: this.#signal } : undefined;
		let promise = this.#innerGetRange(key, { suffixLength }, opts)
			.then((data) => {
				this.#inflight.delete(cacheKey);
				return data;
			})
			.catch((err) => {
				this.#inflight.delete(cacheKey);
				throw err;
			});
		this.#inflight.set(cacheKey, promise);
		return promise;
	}

	async #flush(): Promise<void> {
		let work = new Map(this.#pending);
		this.#pending.clear();
		this.#scheduled = false;

		let opts = this.#signal ? { signal: this.#signal } : undefined;
		let pathPromises: Promise<void>[] = [];
		for (let [path, requests] of work) {
			requests.sort((a, b) => a.offset - b.offset);
			let groups = groupRequests(requests, this.#gapThreshold);
			this.#stats.mergedRequests += groups.length;
			pathPromises.push(this.#fetchGroups(path, groups, opts));
		}
		await Promise.all(pathPromises);
	}

	async #fetchGroups(
		path: AbsolutePath,
		groups: RangeGroup[],
		options?: RequestInit,
	): Promise<void> {
		await Promise.all(
			groups.map(async (group) => {
				try {
					let data = await this.#innerGetRange(
						path,
						{ offset: group.offset, length: group.length },
						options,
					);
					for (let req of group.requests) {
						if (!data) {
							req.resolve(undefined);
							continue;
						}
						let start = req.offset - group.offset;
						let slice = data.slice(start, start + req.length);
						req.resolve(slice);
					}
				} catch (err) {
					for (let req of group.requests) {
						req.reject(err as Error);
					}
				}
			}),
		);
	}
}

/**
 * Wraps a store with range-batching: concurrent `getRange()` calls within a
 * single microtask are merged into fewer HTTP requests.
 *
 * ```typescript
 * import * as zarr from "zarrita";
 *
 * let store = zarr.withRangeBatching(new zarr.FetchStore("https://example.com/data.zarr"));
 * ```
 */
export function withRangeBatching(
	store: AsyncReadable<RequestInit>,
	options?: RangeBatchingOptions,
): BatchedRangeStore {
	return new BatchedRangeStore(store, options);
}
