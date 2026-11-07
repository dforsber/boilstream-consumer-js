// An ephemeral CA is trusted only by the positive-test child process.
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const dir = mkdtempSync(join(tmpdir(), 'boilstream-sse-tls-'));
const cert = join(dir, 'cert.pem'), key = join(dir, 'key.pem');
function run(command, args, env = process.env) {
  const child = spawnSync(command, args, { env, stdio: command === 'openssl' ? 'pipe' : 'inherit' });
  if (child.status !== 0 && child.stderr) process.stderr.write(child.stderr);
  if (child.error) throw child.error;
  if (child.status !== 0) throw new Error(`${command} failed (${child.status})`);
}
try {
  run('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
    '-subj', '/CN=localhost', '-addext', 'subjectAltName=IP:127.0.0.1,DNS:localhost',
    '-keyout', key, '-out', cert]);
  const env = { ...process.env, SSE_TEST_CERT: cert, SSE_TEST_KEY: key };
  delete env.NODE_TLS_REJECT_UNAUTHORIZED;
  delete env.NODE_EXTRA_CA_CERTS;
  run(process.execPath, ['--test', 'test/transport.integration.test.js', 'test/snapshot.integration.test.js'],
    { ...env, SSE_TEST_UNTRUSTED: '1' });
  run(process.execPath, ['--test', 'test/transport.integration.test.js', 'test/snapshot.integration.test.js'],
    { ...env, NODE_EXTRA_CA_CERTS: cert });
} finally {
  rmSync(dir, { recursive: true, force: true });
}
