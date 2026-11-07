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
