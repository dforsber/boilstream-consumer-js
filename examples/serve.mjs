// Serve only the example, actual SDK and installed lockfile dependencies on loopback.
import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
const files = new Map([
  ['/', ['browser-test.html', 'text/html']],
  ['/examples/browser-demo.js', ['browser-demo.js', 'text/javascript']],
  ...['index', 'consumer', 'decoder', 'snapshot-consumer'].map(name =>
    [`/src/${name}.js`, [`../src/${name}.js`, 'text/javascript']]),
  ['/vendor/flechette.mjs', ['../node_modules/@uwdata/flechette/dist/flechette.mjs', 'text/javascript']],
  ...['index', 'parse', 'errors'].map(name =>
    [`/vendor/parser/${name}.js`, [`../node_modules/eventsource-parser/dist/${name}.js`, 'text/javascript']]),
]);
export function serveExample(req, res) {
  const file = files.get(req.url);
  if (!file) { res.writeHead(404); res.end(); return; }
  try {
    const content = readFileSync(new URL(file[0], import.meta.url));
    res.writeHead(200, { 'content-type': file[1], 'cache-control': 'no-store', 'referrer-policy': 'no-referrer' });
    res.end(content);
  } catch { res.writeHead(500); res.end('Run npm ci before starting the example.'); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  createServer(serveExample).listen(8787, '127.0.0.1', () =>
    console.log('SSE v2 preview example: http://127.0.0.1:8787 (configure server CORS for this origin)'));
}
