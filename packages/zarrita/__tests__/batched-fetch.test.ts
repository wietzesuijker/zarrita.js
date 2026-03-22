import type { AbsolutePath, RangeQuery } from "@zarrita/storage";
import { describe, expect, it, vi } from "vitest";
import { withRangeBatching } from "../src/batched-fetch.js";

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

describe("withRangeBatching", () => {
	describe("get() pass-through", () => {
		it("delegates to inner store", async () => {
			let inner = fakeStore();
			let expected = new Uint8Array([1, 2, 3]);
			inner.get.mockResolvedValueOnce(expected);
			let store = withRangeBatching(inner);
			let result = await store.get("/some/path");
			expect(inner.get).toHaveBeenCalledOnce();
			expect(result).toBe(expected);
		});
	});

	describe("getRange() batching", () => {
		it("batches concurrent getRange calls into a single merged fetch", async () => {
			let inner = fakeStore();
			let store = withRangeBatching(inner);

			let [r1, r2, r3] = await Promise.all([
				store.getRange("/data/chunk", { offset: 0, length: 100 }),
				store.getRange("/data/chunk", { offset: 100, length: 100 }),
				store.getRange("/data/chunk", { offset: 200, length: 100 }),
			]);

			expect(inner.getRange).toHaveBeenCalledOnce();
			let call = inner.getRange.mock.calls[0];
			expect(call[1]).toEqual({ offset: 0, length: 300 });

			expect(r1?.length).toBe(100);
			expect(r2?.length).toBe(100);
			expect(r3?.length).toBe(100);
			expect(r1?.[0]).toBe(0);
			expect(r2?.[0]).toBe(100);
			expect(r3?.[0]).toBe(200);

			expect(store.stats.batchedRequests).toBe(3);
			expect(store.stats.mergedRequests).toBe(1);
		});

		it("splits ranges with large gaps into separate groups", async () => {
			let inner = fakeStore();
			let store = withRangeBatching(inner);

			await Promise.all([
				store.getRange("/data/chunk", { offset: 0, length: 100 }),
				store.getRange("/data/chunk", { offset: 200000, length: 100 }),
			]);

			expect(inner.getRange).toHaveBeenCalledTimes(2);
			expect(store.stats.mergedRequests).toBe(2);
		});

		it("groups requests from different paths independently", async () => {
			let inner = fakeStore();
			let store = withRangeBatching(inner);

			await Promise.all([
				store.getRange("/data/a", { offset: 0, length: 100 }),
				store.getRange("/data/b", { offset: 0, length: 100 }),
			]);

			expect(inner.getRange).toHaveBeenCalledTimes(2);
		});

		it("handles overlapping ranges correctly", async () => {
			let inner = fakeStore();
			let store = withRangeBatching(inner);

			let [r1, r2] = await Promise.all([
				store.getRange("/data/chunk", { offset: 0, length: 100 }),
				store.getRange("/data/chunk", { offset: 50, length: 100 }),
			]);

			expect(inner.getRange).toHaveBeenCalledOnce();
			let call = inner.getRange.mock.calls[0];
			expect(call[1]).toEqual({ offset: 0, length: 150 });

			expect(r1?.length).toBe(100);
			expect(r2?.length).toBe(100);
			expect(r1?.[0]).toBe(0);
			expect(r2?.[0]).toBe(50);
		});

		it("resolves concurrent identical offset ranges correctly", async () => {
			let inner = fakeStore();
			let store = withRangeBatching(inner);

			let [r1, r2] = await Promise.all([
				store.getRange("/data/chunk", { offset: 0, length: 100 }),
				store.getRange("/data/chunk", { offset: 0, length: 100 }),
			]);

			expect(inner.getRange).toHaveBeenCalledOnce();
			expect(r1?.length).toBe(100);
			expect(r2?.length).toBe(100);
			expect(r1?.[0]).toBe(0);
			expect(r2?.[0]).toBe(0);
		});

		it("respects custom gapThreshold", async () => {
			let inner = fakeStore();
			let store = withRangeBatching(inner, { gapThreshold: 0 });

			await Promise.all([
				store.getRange("/data/chunk", { offset: 0, length: 100 }),
				store.getRange("/data/chunk", { offset: 101, length: 100 }),
			]);

			expect(inner.getRange).toHaveBeenCalledTimes(2);
		});
	});

	describe("suffix pass-through", () => {
		it("passes suffix requests through unbatched", async () => {
			let inner = fakeStore();
			let store = withRangeBatching(inner);

			let result = await store.getRange("/data/shard", { suffixLength: 1024 });

			expect(inner.getRange).toHaveBeenCalledOnce();
			expect(inner.getRange.mock.calls[0][1]).toEqual({ suffixLength: 1024 });
			expect(result?.length).toBe(1024);
		});

		it("deduplicates concurrent suffix requests", async () => {
			let inner = fakeStore();
			let store = withRangeBatching(inner);

			let [r1, r2] = await Promise.all([
				store.getRange("/data/shard", { suffixLength: 1024 }),
				store.getRange("/data/shard", { suffixLength: 1024 }),
			]);

			expect(inner.getRange).toHaveBeenCalledOnce();
			expect(r1?.length).toBe(1024);
			expect(r2?.length).toBe(1024);
		});

		it("evicts failed suffix from inflight", async () => {
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
			let store = withRangeBatching(inner);

			await expect(
				store.getRange("/data/shard", { suffixLength: 1024 }),
			).rejects.toThrow("transient error");

			let result = await store.getRange("/data/shard", { suffixLength: 1024 });
			expect(result?.length).toBe(1024);
		});
	});

	describe("error handling", () => {
		it("rejects all requests in a group when fetch fails", async () => {
			let inner = fakeStore();
			inner.getRange.mockRejectedValue(new Error("Network error"));
			let store = withRangeBatching(inner);

			let results = await Promise.allSettled([
				store.getRange("/data/chunk", { offset: 0, length: 100 }),
				store.getRange("/data/chunk", { offset: 100, length: 100 }),
			]);

			expect(results[0].status).toBe("rejected");
			expect(results[1].status).toBe("rejected");
			expect((results[0] as PromiseRejectedResult).reason.message).toBe(
				"Network error",
			);
		});

		it("resolves with undefined when inner returns undefined", async () => {
			let inner = fakeStore();
			inner.getRange.mockResolvedValue(undefined);
			let store = withRangeBatching(inner);

			let result = await store.getRange("/data/chunk", {
				offset: 0,
				length: 100,
			});
			expect(result).toBeUndefined();
		});
	});

	describe("abort signal", () => {
		it("passes constructor signal to inner getRange", async () => {
			let inner = fakeStore();
			let controller = new AbortController();
			let store = withRangeBatching(inner, { signal: controller.signal });

			await store.getRange("/data/chunk", { offset: 0, length: 100 });

			let call = inner.getRange.mock.calls[0];
			expect(call[2]).toEqual({ signal: controller.signal });
		});

		it("passes constructor signal on suffix pass-through", async () => {
			let inner = fakeStore();
			let controller = new AbortController();
			let store = withRangeBatching(inner, { signal: controller.signal });

			await store.getRange("/data/shard", { suffixLength: 1024 });

			let call = inner.getRange.mock.calls[0];
			expect(call[2]).toEqual({ signal: controller.signal });
		});
	});
});
