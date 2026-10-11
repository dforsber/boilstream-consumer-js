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
