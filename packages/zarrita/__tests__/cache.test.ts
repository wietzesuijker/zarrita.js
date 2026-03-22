import type { AbsolutePath, RangeQuery } from "@zarrita/storage";
import { describe, expect, it, vi } from "vitest";
import { withCache } from "../src/cache.js";

function fakeStore() {
	return {
		get: vi.fn((_key: AbsolutePath, _opts?: RequestInit) =>
			Promise.resolve<Uint8Array | undefined>(new Uint8Array(0)),
		),
		getRange: vi.fn(
			(
				_key: AbsolutePath,
				range: RangeQuery,
				_options?: RequestInit,
			): Promise<Uint8Array | undefined> => {
				if ("suffixLength" in range) {
					return Promise.resolve(new Uint8Array(range.suffixLength));
				}
				let buf = new Uint8Array(range.length);
				for (let i = 0; i < buf.length; i++) {
					buf[i] = (range.offset + i) & 0xff;
				}
				return Promise.resolve(buf);
			},
		),
	};
}

describe("withCache", () => {
	describe("get() pass-through", () => {
		it("delegates to inner store without caching", async () => {
			let inner = fakeStore();
			let expected = new Uint8Array([1, 2, 3]);
			inner.get.mockResolvedValueOnce(expected);
			let store = withCache(inner);
			let result = await store.get("/some/path");
			expect(inner.get).toHaveBeenCalledOnce();
			expect(result).toBe(expected);
		});
	});

	describe("offset range caching", () => {
		it("returns cached data on second request", async () => {
			let inner = fakeStore();
			let store = withCache(inner);

			await store.getRange("/data/chunk", { offset: 0, length: 100 });
			let r2 = await store.getRange("/data/chunk", { offset: 0, length: 100 });

			expect(inner.getRange).toHaveBeenCalledOnce();
			expect(store.stats.hits).toBe(1);
			expect(store.stats.misses).toBe(1);
			expect(r2?.length).toBe(100);
		});

		it("caches undefined (404s)", async () => {
			let inner = fakeStore();
			inner.getRange.mockResolvedValue(undefined);
			let store = withCache(inner);

			await store.getRange("/data/missing", { offset: 0, length: 100 });
			let r2 = await store.getRange("/data/missing", { offset: 0, length: 100 });

			expect(inner.getRange).toHaveBeenCalledOnce();
			expect(r2).toBeUndefined();
			expect(store.stats.hits).toBe(1);
		});
	});

	describe("suffix range caching", () => {
		it("caches suffix range requests", async () => {
			let inner = fakeStore();
			let store = withCache(inner);

			await store.getRange("/data/shard", { suffixLength: 1024 });
			await store.getRange("/data/shard", { suffixLength: 1024 });

			expect(inner.getRange).toHaveBeenCalledOnce();
			expect(store.stats.hits).toBe(1);
		});

		it("deduplicates concurrent suffix requests", async () => {
			let inner = fakeStore();
			let store = withCache(inner);

			let [r1, r2] = await Promise.all([
				store.getRange("/data/shard", { suffixLength: 1024 }),
				store.getRange("/data/shard", { suffixLength: 1024 }),
			]);

			expect(inner.getRange).toHaveBeenCalledOnce();
			expect(r1?.length).toBe(1024);
			expect(r2?.length).toBe(1024);
			expect(store.stats.hits).toBe(1);
			expect(store.stats.misses).toBe(1);
		});

		it("evicts failed suffix request from inflight cache", async () => {
			let inner = fakeStore();
			let callCount = 0;
			inner.getRange.mockImplementation(
				(
					_key: AbsolutePath,
					range: RangeQuery,
					_options?: RequestInit,
				): Promise<Uint8Array | undefined> => {
					callCount++;
					if (callCount === 1 && "suffixLength" in range) {
						return Promise.reject(new Error("transient error"));
					}
					if ("suffixLength" in range) {
						return Promise.resolve(new Uint8Array(range.suffixLength));
					}
					return Promise.resolve(new Uint8Array(0));
				},
			);
			let store = withCache(inner);

			await expect(
				store.getRange("/data/shard", { suffixLength: 1024 }),
			).rejects.toThrow("transient error");

			let result = await store.getRange("/data/shard", { suffixLength: 1024 });
			expect(result?.length).toBe(1024);
		});
	});

	describe("LRU eviction", () => {
		it("evicts oldest entry when capacity exceeded", async () => {
			let inner = fakeStore();
			let store = withCache(inner, { cacheSize: 2 });

			await store.getRange("/a", { offset: 0, length: 10 });
			await store.getRange("/b", { offset: 0, length: 10 });
			await store.getRange("/c", { offset: 0, length: 10 });

			inner.getRange.mockClear();
			await store.getRange("/b", { offset: 0, length: 10 });
			expect(inner.getRange).not.toHaveBeenCalled();

			await store.getRange("/a", { offset: 0, length: 10 });
			expect(inner.getRange).toHaveBeenCalledOnce();
		});

		it("honors cacheSize option", async () => {
			let inner = fakeStore();
			let store = withCache(inner, { cacheSize: 1 });

			await store.getRange("/a", { offset: 0, length: 10 });
			await store.getRange("/b", { offset: 0, length: 10 });

			inner.getRange.mockClear();
			await store.getRange("/a", { offset: 0, length: 10 });
			expect(inner.getRange).toHaveBeenCalledOnce();
		});
	});

	describe("clear()", () => {
		it("empties cache and inflight map", async () => {
			let inner = fakeStore();
			let store = withCache(inner);

			await store.getRange("/data/chunk", { offset: 0, length: 100 });
			expect(store.stats.hits).toBe(0);

			store.clear();

			await store.getRange("/data/chunk", { offset: 0, length: 100 });
			expect(inner.getRange).toHaveBeenCalledTimes(2);
			expect(store.stats.misses).toBe(2);
		});
	});

	describe("options pass-through", () => {
		it("passes options to inner store getRange", async () => {
			let inner = fakeStore();
			let store = withCache(inner);
			let opts: RequestInit = { headers: { "X-Test": "1" } };

			await store.getRange("/data/chunk", { offset: 0, length: 100 }, opts);
			expect(inner.getRange).toHaveBeenCalledWith(
				"/data/chunk",
				{ offset: 0, length: 100 },
				opts,
			);
		});
	});
});
