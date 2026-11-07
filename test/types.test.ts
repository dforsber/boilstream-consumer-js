import { BoilStreamConsumer, type ReplayCursor, type RefreshRequired } from "boilstream-consumer";
import { EventSource as NodeEventSource } from "eventsource";

const options = {
  onSchema: (_schema: unknown, cursor: ReplayCursor) => { const exact: string = cursor; void exact; },
  onRefreshRequired: (details: RefreshRequired) => { const reason: "refresh_required" = details.reason; void reason; },
};
new BoilStreamConsumer("https://example.invalid", { ...options, EventSource: NodeEventSource });
new BoilStreamConsumer("https://example.invalid", { ...options, EventSource });
new BoilStreamConsumer("https://example.invalid", {
  onBatch(table, cursor) {
    const exact: string = cursor;
    const rows: number = table.numRows;
    // @ts-expect-error Opaque cursor must not be treated as a numeric sequence.
    const numeric: number = cursor;
    void [exact, rows, numeric];
  },
});


import { BoilStreamSnapshotConsumer, StreamError, type SnapshotConsumerOptions } from "boilstream-consumer";
const snapshotOptions: SnapshotConsumerOptions = {
  getToken: async ({ signal, unauthorized }) => { void [signal.aborted, unauthorized]; return "a".repeat(64); },
  onSnapshotStart: async ({ signal }) => { void signal; },
  onSnapshot: async (table, { signal }) => { const rows: number = table.numRows; void [rows, signal]; },
  onSnapshotComplete: async (cursor, { signal }) => { const exact: string = cursor; void [exact, signal]; },
  onSnapshotDiscard: async () => {},
  onBatch: async (table, cursor, { signal }) => { void [table.numRows, cursor, signal]; },
  onRefreshRequired: async ({ reason }) => { const refresh: "refresh_required" = reason; void refresh; },
  onSchema: async (schema, cursor) => { const exact: string | undefined = cursor; void [schema.fields, exact]; },
};
const snapshot = new BoilStreamSnapshotConsumer("https://example.invalid/stream/v2", snapshotOptions);
const running: Promise<void> = snapshot.run();
const committed: string | undefined = snapshot.cursor;
const errorCode: string = new StreamError("protocol_failed").code;
void [running, committed, errorCode];
// @ts-expect-error Snapshot activation and refresh callbacks are required.
new BoilStreamSnapshotConsumer("https://example.invalid/stream/v2", { getToken: () => "a".repeat(64) });
