'use strict';

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const frames = require('rhea/lib/frames.js');
const message = require('rhea/lib/message.js');
const types = require('rhea/lib/types.js');

const root = path.resolve(__dirname, '..');
const args = ['run', 'examples/recovery', '--target', 'js', '--deny-warn'];
const useWindowsAdapter = process.platform === 'win32' && !process.env.MOON_EXE && !process.env.MOONC_OVERRIDE;
const command = useWindowsAdapter ? 'pwsh' : process.env.MOON_EXE || 'moon';
const commandArgs = useWindowsAdapter
  ? ['-NoProfile', '-File', path.join(root, 'tools/windows/moon.ps1'), ...args]
  : args;
const result = spawnSync(command, commandArgs, { cwd: root, encoding: 'utf8', timeout: 30000 });
assert.equal(result.status, 0, result.error || result.stderr);

const trace = new Map();
for (const line of result.stdout.split(/\r?\n/)) {
  const match = /^(\w+)=([\da-f]+)$/.exec(line);
  if (match) trace.set(match[1], Buffer.from(match[2], 'hex'));
}

function read(label, descriptor) {
  const wire = trace.get(label);
  assert.ok(wire, 'missing trace step ' + label);
  const sequence = [];
  for (let offset = 0; offset < wire.length;) {
    const size = wire.readUInt32BE(offset);
    assert.ok(size >= 8 && size <= 620);
    const bytes = wire.subarray(offset, offset + size);
    const frame = frames.read_frame(bytes);
    assert.equal(frame.performative.constructor.descriptor.numeric, descriptor);
    const body = new types.Reader(bytes.subarray(bytes[4] * 4)).read().value;
    sequence.push({ perf: frame.performative, payload: frame.payload, body });
    offset += size;
  }
  return sequence;
}

const firstA = read('a_attach1', 0x12)[0];
const firstB = read('b_attach1', 0x12)[0];
const secondA = read('a_attach2', 0x12)[0];
const secondB = read('b_attach2', 0x12)[0];
function unsigned(value) {
  return Buffer.isBuffer(value) ? Number(value.readBigUInt64BE()) : Number(value);
}

const a = 'a'.repeat(32);
const b = 'b'.repeat(32);
const tags = attach => attach.body[7].value.filter((_, index) => index % 2 === 0)
  .map(tag => tag.value.toString('ascii'));
assert.deepEqual(tags(firstA), [a, b]);
assert.equal(Boolean(firstA.perf.incomplete_unsettled), false);
assert.deepEqual(tags(firstB), [a]);
assert.equal(Boolean(firstB.perf.incomplete_unsettled), true);
assert.deepEqual(tags(secondA), [b]);
assert.deepEqual(tags(secondB), [b]);
assert.equal(Boolean(secondB.perf.incomplete_unsettled), false);
assert.notEqual(firstA.perf.handle, secondA.perf.handle);

const early = read('a_recover_a', 0x14)[0];
assert.equal(Boolean(early.perf.resume), true);
assert.equal(Boolean(early.perf.settled), true);
assert.equal(early.perf.delivery_tag.toString('ascii'), a);
assert.equal(early.perf.delivery_id, 0);
assert.equal(unsigned(early.body[7].descriptor.value), 0x24);
assert.equal(early.payload?.length || 0, 0);
assert.equal(read('a_detach', 0x16)[0].perf.closed || false, false);
assert.equal(read('b_detach', 0x16)[0].perf.closed || false, false);

const oldPrefix = read('prefix', 0x14)[0].payload;
const resume = read('a_recover_b', 0x14);
assert.equal(Boolean(resume[0].perf.resume), true);
assert.equal(resume[0].perf.delivery_id, 1);
assert.equal(resume[0].perf.delivery_tag.toString('ascii'), b);
const received = resume[0].body[7];
assert.equal(unsigned(received.descriptor.value), 0x23);
assert.equal(received.value[0].value, 0);
assert.equal(unsigned(received.value[1].value), oldPrefix.length);
const restored = message.decode(Buffer.concat([oldPrefix, ...resume.map(frame => frame.payload)]));
assert.deepEqual(restored.body.content, Buffer.alloc(1400, 0x78));

const receiver = read('b_disposition', 0x15)[0].perf;
const sender = read('a_disposition', 0x15)[0].perf;
assert.deepEqual([Boolean(receiver.role), receiver.first, Boolean(receiver.settled)], [true, 1, false]);
assert.deepEqual([Boolean(sender.role), sender.first, Boolean(sender.settled)], [false, 1, true]);
assert.equal(Boolean(read('new_delivery', 0x14)[0].perf.resume), false);
console.log('Rhea decoded both recovery rounds, Accepted, exact payload and final Second settlement.');
