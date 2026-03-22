import type { AbsolutePath, AsyncReadable, RangeQuery } from "@zarrita/storage";
import { LRUCache } from "./lru-cache.js";

export interface CacheOptions {
	cacheSize?: number;
}

export interface CacheStats {
	hits: number;
	misses: number;
}

export class CachedStore implements AsyncReadable<RequestInit> {
	#inner: AsyncReadable<RequestInit>;
	#innerGetRange: NonNullable<AsyncReadable<RequestInit>["getRange"]>;
	#cache: LRUCache<Uint8Array | undefined>;
	#inflight = new Map<string, Promise<Uint8Array | undefined>>();
	#stats: CacheStats = { hits: 0, misses: 0 };

	get stats(): Readonly<CacheStats> {
		return { ...this.#stats };
	}

	constructor(
		inner: AsyncReadable<RequestInit>,
		options?: CacheOptions,
	) {
		if (!inner.getRange) {
			throw new Error("CachedStore requires a store with getRange");
		}
		this.#inner = inner;
		this.#innerGetRange = inner.getRange.bind(inner);
		this.#cache = new LRUCache(options?.cacheSize ?? 256);
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
		options?: RequestInit,
	): Promise<Uint8Array | undefined> {
		if ("suffixLength" in range) {
			return this.#getSuffix(key, range.suffixLength, options);
		}

		let { offset, length } = range;
		let cacheKey = `${key}:${offset}:${length}`;

		if (this.#cache.has(cacheKey)) {
			this.#stats.hits++;
			return Promise.resolve(this.#cache.get(cacheKey));
		}

		this.#stats.misses++;
		return this.#innerGetRange(key, range, options).then((data) => {
			this.#cache.set(cacheKey, data);
			return data;
		});
	}

	#getSuffix(
		key: AbsolutePath,
		suffixLength: number,
		options?: RequestInit,
	): Promise<Uint8Array | undefined> {
		let cacheKey = `${key}:suffix:${suffixLength}`;

		if (this.#cache.has(cacheKey)) {
			this.#stats.hits++;
			return Promise.resolve(this.#cache.get(cacheKey));
		}

		let inflight = this.#inflight.get(cacheKey);
		if (inflight) {
			this.#stats.hits++;
			return inflight;
		}

		this.#stats.misses++;
		let promise = this.#innerGetRange(key, { suffixLength }, options)
			.then((data) => {
				this.#cache.set(cacheKey, data);
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

	clear(): void {
		this.#cache.clear();
		this.#inflight.clear();
	}
}

export function withCache(
	store: AsyncReadable<RequestInit>,
	options?: CacheOptions,
): CachedStore {
	return new CachedStore(store, options);
}
