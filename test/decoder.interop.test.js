import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Decimal, Int64, TimestampNanosecond, makeVector, vectorFromArray, tableFromArrays, tableToIPC } from 'apache-arrow';
import { decodeArrowIPC } from 'boilstream-consumer';

const decode = (columns, options) => decodeArrowIPC(Buffer.from(tableToIPC(tableFromArrays(columns), 'stream')).toString('base64'), options);

test('independent Arrow int64 nullable duplicates retain exact values', () => {
  const values = [9007199254740993n, null, -9007199254740993n, 9007199254740993n];
  const table = decode({ exact: vectorFromArray(values, new Int64()) });
  assert.deepEqual([...table.getChild('exact')], values);
});

test('independent Arrow decimal retains coefficient and scale without float rounding', () => {
  const coefficient = 123456789012345678901n;
  const words = Uint32Array.from([0n, 32n, 64n, 96n].map(shift => Number((coefficient >> shift) & 0xffffffffn)));
  const table = decode({ money: vectorFromArray([words, null, words], new Decimal(6, 38)) });
  assert.deepEqual([...table.getChild('money')], [coefficient, null, coefficient]);
  assert.equal(table.schema.fields[0].type.scale, 6);
});

test('independent Arrow nanosecond timestamps retain units, timezone and precision', () => {
  const values = [1794000000123456789n, -1n];
  const vector = makeVector({ type: new TimestampNanosecond('UTC'), data: BigInt64Array.from(values) });
  const table = decode({ at: vector });
  assert.deepEqual([...table.getChild('at')], values);
  assert.equal(table.schema.fields[0].type.timezone, 'UTC');
  assert.equal(table.schema.fields[0].type.unit, 3); // Arrow TimeUnit.NANOSECOND
});

test('number conversion is an explicit override for safe int64 values', () => {
  const table = decode({ exact: BigInt64Array.from([11n, -7n]) }, { useBigInt: false });
  assert.deepEqual([...table.getChild('exact')], [11, -7]);
});


test('malformed base64 and truncated IPC cannot become an acknowledged empty table', () => {
  const stream = Buffer.from(tableToIPC(tableFromArrays({ value: [1, null, 1] }), 'stream'));
  const encoded = stream.toString('base64');
  for (const payload of ['', 'invalid-arrow', encoded + '\n', '!!!!', 'AA=A',
    stream.subarray(0, -1).toString('base64'),
    stream.subarray(0, -8).toString('base64'),
    Buffer.from('ffffffff00000000ffffffff00000000', 'hex').toString('base64'),
    Buffer.alloc(16 * 1024 * 1024 + 1).toString('base64'),
  ]) assert.throws(() => decodeArrowIPC(payload));
  assert.deepEqual([...decodeArrowIPC(encoded).getChild('value')], [1, null, 1]);
});

test('an independently encoded empty schema remains a valid empty table', () => {
  const table = tableFromArrays({ value: BigInt64Array.from([1n]) }).slice(0, 0);
  const decoded = decodeArrowIPC(Buffer.from(tableToIPC(table, 'stream')).toString('base64'));
  assert.equal(decoded.numRows, 0);
  assert.equal(decoded.schema.fields[0].name, 'value');
});
