import { afterEach, describe, expect, it, vi } from "bun:test";
import { CollabSocket, HOST_RECLAIM_BACKOFF_MAX_MS, HOST_RECLAIM_WINDOW_MS } from "../../src/collab/relay-client";

const NativeWebSocket = globalThis.WebSocket;

class ScriptedWebSocket {
	static readonly CONNECTING = 0;
	static readonly OPEN = 1;
	static readonly CLOSING = 2;
	static readonly CLOSED = 3;
	static instances: ScriptedWebSocket[] = [];

	readonly url: string;
	binaryType = "arraybuffer";
	bufferedAmount = 0;
	onclose: ((event: CloseEvent) => void) | null = null;
	onerror: ((event: Event) => void) | null = null;
	onmessage: ((event: MessageEvent) => void) | null = null;
	onopen: ((event: Event) => void) | null = null;
	readyState = ScriptedWebSocket.CONNECTING;

	constructor(url: string | URL) {
		this.url = String(url);
		ScriptedWebSocket.instances.push(this);
	}

	send(_data: unknown): void {}

	open(): void {
		this.readyState = ScriptedWebSocket.OPEN;
		this.onopen?.(new Event("open"));
	}

	relayClose(code: number, reason: string): void {
		this.readyState = ScriptedWebSocket.CLOSED;
		this.onclose?.(new CloseEvent("close", { code, reason }));
	}

	close(code = 1000, reason = "closed"): void {
		if (this.readyState === ScriptedWebSocket.CLOSED) return;
		this.relayClose(code, reason);
	}
}

function installScriptedWebSocket(): void {
	ScriptedWebSocket.instances = [];
	Object.defineProperty(globalThis, "WebSocket", { configurable: true, value: ScriptedWebSocket });
}

function restoreNativeWebSocket(): void {
	Object.defineProperty(globalThis, "WebSocket", { configurable: true, value: NativeWebSocket });
}

function guestSocket(key: CryptoKey): CollabSocket {
	return new CollabSocket({
		wsUrl: "ws://localhost:8788/r/transient-network-room",
		role: "guest",
		key,
	});
}

function hostSocket(key: CryptoKey): CollabSocket {
	return new CollabSocket({
		wsUrl: "ws://localhost:8788/r/transient-network-room",
		role: "host",
		key,
	});
}

/** The relay completes the upgrade, then refuses a second host for the room. */
function refuseDuplicateHost(index: number): void {
	instance(index).open();
	instance(index).relayClose(4009, "a host is already connected for this room");
}

function instance(index: number): ScriptedWebSocket {
	const ws = ScriptedWebSocket.instances[index];
	if (!ws) throw new Error(`WebSocket instance ${index} was not created`);
	return ws;
}

afterEach(() => {
	restoreNativeWebSocket();
	vi.restoreAllMocks();
	vi.useRealTimers();
});

describe("CollabSocket guest room recovery", () => {
	it("retries a host-closed room and missing-room races until the host returns", async () => {
		vi.useFakeTimers();
		vi.spyOn(Math, "random").mockReturnValue(0.5);
		installScriptedWebSocket();
		const key = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]);
		const closes: Array<{ reason: string; willReconnect: boolean }> = [];
		const socket = guestSocket(key);
		socket.onClose = (reason, willReconnect) => closes.push({ reason, willReconnect });

		try {
			socket.connect();
			instance(0).open();
			instance(0).relayClose(4001, "room closed");
			expect(closes).toEqual([{ reason: "room closed", willReconnect: true }]);

			vi.advanceTimersByTime(1_000);
			instance(1).open();
			instance(1).relayClose(4004, "no such room");
			expect(closes.at(-1)).toEqual({ reason: "no such room", willReconnect: true });

			vi.advanceTimersByTime(1_000);
			expect(ScriptedWebSocket.instances).toHaveLength(2);
			vi.advanceTimersByTime(1_000);
			instance(2).open();
			expect(socket.isOpen).toBe(true);
		} finally {
			socket.close();
		}
	});

	it("keeps a missing room terminal on the initial join", async () => {
		vi.useFakeTimers();
		installScriptedWebSocket();
		const key = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]);
		const closes: Array<{ reason: string; willReconnect: boolean }> = [];
		const socket = guestSocket(key);
		socket.onClose = (reason, willReconnect) => closes.push({ reason, willReconnect });

		socket.connect();
		instance(0).open();
		instance(0).relayClose(4004, "no such room");

		expect(closes).toEqual([{ reason: "no such room", willReconnect: false }]);
		vi.advanceTimersByTime(30_000);
		expect(ScriptedWebSocket.instances).toHaveLength(1);
	});
});

describe("CollabSocket host room recovery", () => {
	it("reclaims its room while the relay still holds the dropped connection", async () => {
		vi.useFakeTimers();
		vi.spyOn(Math, "random").mockReturnValue(0.5);
		installScriptedWebSocket();
		const key = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]);
		const reconnects: boolean[] = [];
		const socket = hostSocket(key);
		socket.onClose = (_reason, willReconnect) => reconnects.push(willReconnect);

		try {
			socket.connect();
			instance(0).open();
			// The client sees its connection end before the relay retires it.
			instance(0).relayClose(1006, "Connection ended");
			// Refusals back off 1 s, 2 s, 4 s, then hold at the 5 s reclaim cap, even though each one opened first.
			for (const [index, delay] of [
				[1, 1_000],
				[2, 1_000],
				[3, 2_000],
				[4, 4_000],
				[5, HOST_RECLAIM_BACKOFF_MAX_MS],
				[6, HOST_RECLAIM_BACKOFF_MAX_MS],
			] as const) {
				vi.advanceTimersByTime(delay - 1);
				expect(ScriptedWebSocket.instances).toHaveLength(index);
				vi.advanceTimersByTime(1);
				if (index < 6) refuseDuplicateHost(index);
			}
			instance(6).open();
			expect(socket.isOpen).toBe(true);
			expect(reconnects).toEqual([true, true, true, true, true, true]);
		} finally {
			socket.close();
		}
	});

	it("treats a duplicate host as a conflict on first connect and once the reclaim window lapses", async () => {
		vi.useFakeTimers();
		vi.spyOn(Math, "random").mockReturnValue(0.5);
		installScriptedWebSocket();
		const key = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]);
		const first = hostSocket(key);
		const firstReconnects: boolean[] = [];
		first.onClose = (_reason, willReconnect) => firstReconnects.push(willReconnect);
		first.connect();
		refuseDuplicateHost(0);
		expect(firstReconnects).toEqual([false]);
		vi.advanceTimersByTime(30_000);
		expect(ScriptedWebSocket.instances).toHaveLength(1);

		const socket = hostSocket(key);
		const reconnects: boolean[] = [];
		socket.onClose = (_reason, willReconnect) => reconnects.push(willReconnect);
		socket.connect();
		instance(1).open();
		const droppedAt = Date.now();
		instance(1).relayClose(1006, "Connection ended");
		// With jitter pinned, attempts land at the drop's 1 s retry, then the reclaim
		// backoff (1, 2, 4 s, then 5 s each). Every refusal before the window closes
		// reconnects; the first past the 150 s window, at +153 s, is terminal.
		const attemptOffsetsMs = [1_000, 2_000, 4_000, 8_000];
		while (attemptOffsetsMs.at(-1)! < HOST_RECLAIM_WINDOW_MS) {
			attemptOffsetsMs.push(attemptOffsetsMs.at(-1)! + HOST_RECLAIM_BACKOFF_MAX_MS);
		}
		expect(attemptOffsetsMs.at(-1)).toBe(153_000);
		for (const [i, offsetMs] of attemptOffsetsMs.entries()) {
			const index = i + 2;
			vi.advanceTimersByTime(droppedAt + offsetMs - Date.now() - 1);
			expect(ScriptedWebSocket.instances).toHaveLength(index);
			vi.advanceTimersByTime(1);
			expect(ScriptedWebSocket.instances).toHaveLength(index + 1);
			refuseDuplicateHost(index);
			expect(reconnects.at(-1)).toBe(offsetMs < HOST_RECLAIM_WINDOW_MS);
		}
		expect(reconnects).toEqual([true, ...attemptOffsetsMs.map(offsetMs => offsetMs < HOST_RECLAIM_WINDOW_MS)]);
		vi.advanceTimersByTime(60_000);
		expect(ScriptedWebSocket.instances).toHaveLength(attemptOffsetsMs.length + 2);
	});
});
