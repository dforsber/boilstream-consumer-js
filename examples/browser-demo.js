import { BoilStreamSnapshotConsumer } from '/src/index.js';

const element = id => document.getElementById(id);
const limit = 10000, textLimit = 1000000;
const jsonValue = (_, value) => typeof value === 'bigint' ? value.toString() : value;
let active = { rows: [], cursor: undefined, endpoint: undefined };
let staged = [], consumer;
function render() {
  element('rows').textContent = String(active.rows.length);
  element('cursor').textContent = active.cursor ?? 'none';
  element('preview').textContent = JSON.stringify(active.rows.slice(0, 10),
    jsonValue, 2);
}
function append(rows, table) {
  if (rows.length + table.numRows > limit) throw new Error('Demo row limit');
  const columns = table.schema.fields.map(field => [field.name, table.getChild(field.name)]);
  const batch = Array.from({ length: table.numRows }, (_, i) =>
    Object.fromEntries(columns.map(([name, column]) => [name, column.at(i)])));
  const next = rows.concat(batch); // Preserve duplicates; payload equality is not an ID.
  if (JSON.stringify(next, jsonValue).length > textLimit) throw new Error('Demo text limit');
  return next;
}
function buttons(running) {
  for (const id of ['start', 'endpoint', 'token']) element(id).disabled = running;
  element('resume').disabled = running || !active.cursor;
  element('stop').disabled = !running;
}
async function connect(resume) {
  if (consumer) return;
  const endpoint = element('endpoint').value;
  let token = element('token').value.trim();
  if (!token) { element('status').textContent = 'Enter a stream token'; return; }
  if (resume && active.endpoint !== endpoint) {
    element('status').textContent = 'Resume requires the same endpoint'; return;
  }
  if (!resume) { active = { rows: [], cursor: undefined, endpoint }; render(); }
  element('token').value = '';
  buttons(true);
  element('status').textContent = resume ? 'resuming' : 'snapshot pending';
  try {
    consumer = new BoilStreamSnapshotConsumer(endpoint, {
      cursor: resume ? active.cursor : undefined,
      getToken: ({ unauthorized }) => {
        if (unauthorized) throw new Error('Supply a new token manually');
        return token;
      },
      onSnapshotStart() { staged = []; element('status').textContent = 'staging snapshot'; },
      onSnapshot(table) { staged = append(staged, table); },
      onSnapshotComplete(cursor) {
        active = { rows: staged, cursor, endpoint }; staged = []; render();
        element('status').textContent = 'live';
      },
      onSnapshotDiscard() { staged = []; },
      onSchema() { if (resume) element('status').textContent = 'live'; },
      onBatch(table, cursor) {
        // One synchronous assignment commits rows and cursor together for this in-memory demo.
        active = { rows: append(active.rows, table), cursor, endpoint }; render();
      },
      onRefreshRequired() {
        active = { rows: [], cursor: undefined, endpoint }; staged = []; render();
        element('status').textContent = 'refresh required';
      },
    });
    await consumer.run();
    element('status').textContent = 'stopped';
  } catch {
    // Never print the credential, request URL or raw transport exception.
    active = { rows: [], cursor: undefined, endpoint }; staged = []; render();
    element('status').textContent = 'refresh required: check token, endpoint and demo row limit';
  } finally {
    token = ''; consumer = undefined; buttons(false);
  }
}
element('connect').addEventListener('submit', event => { event.preventDefault(); void connect(false); });
element('resume').addEventListener('click', () => { void connect(true); });
element('stop').addEventListener('click', () => consumer?.close());
window.addEventListener('pagehide', () => consumer?.close());
