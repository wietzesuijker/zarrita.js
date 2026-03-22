/**
 * Simple LRU cache using Map insertion order.
 * Oldest entry (first key) is evicted when capacity is exceeded.
 */
export class LRUCache<V> {
	#map = new Map<string, V>();
	#max: number;

	constructor(max: number) {
		this.#max = max;
	}

	has(key: string): boolean {
		return this.#map.has(key);
	}

	get(key: string): V | undefined {
		if (!this.#map.has(key)) {
			return undefined;
		}
		let value = this.#map.get(key) as V;
		this.#map.delete(key);
		this.#map.set(key, value);
		return value;
	}

	set(key: string, value: V): void {
		this.#map.delete(key);
		this.#map.set(key, value);
		if (this.#map.size > this.#max) {
			let first = this.#map.keys().next().value;
			if (first !== undefined) {
				this.#map.delete(first);
			}
		}
	}

	clear(): void {
		this.#map.clear();
	}

	get size(): number {
		return this.#map.size;
	}
}
