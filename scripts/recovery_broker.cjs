'use strict';

const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const net = require('node:net');
const path = require('node:path');
const frames = require('rhea/lib/frames.js');
const message = require('rhea/lib/message.js');
const types = require('rhea/lib/types.js');

const root = path.resolve(__dirname, '..');
const executable = process.env.MOONAMQP1_EXE || path.join(
  root, '_build', 'native', 'debug', 'build', 'cmd', 'main', 'main.exe',
);
const header = Buffer.from('414d515000010000', 'hex');
const tagA = Buffer.alloc(32, 97);
const tagB = Buffer.alloc(32, 98);
const tagC = Buffer.from('new');
const body = Buffer.alloc(1400, 120);
const empty = types.Null();
const uint = types.wrap_uint;
const ulong = types.wrap_ulong;
const byte = types.wrap_ubyte;
const bool = types.wrap_boolean;
const text = types.wrap_string;
const binary = types.wrap_binary;
const described = (code, fields) => types.described(ulong(code), types.wrap_list(fields));
const accepted = described(0x24, []);
const received = offset => described(0x23, [uint(0), ulong(offset)]);
const unsettled = pairs => types.Map32(pairs.flatMap(([tag, state]) => [binary(tag), state]));

function entries(field) {
  const list = field.value;
  const result = new Map();
  for (let i = 0; i < list.length; i += 2) {
    result.set(list[i].value.toString('ascii'), list[i + 1]);
  }
  return result;
}

function number(value) {
  return Buffer.isBuffer(value) ? Number(value.readBigUInt64BE()) : Number(value);
}

function offset(state) {
  assert.equal(number(state.descriptor.value), 0x23);
  assert.equal(state.value[0].value, 0);
  return number(state.value[1].value);
}

function data(bytes) {
  const writer = new types.Writer();
  writer.write(types.described(ulong(0x75), binary(bytes)));
  return writer.toBuffer();
}

class WirePeer {
  constructor(socket) {
    this.socket = socket;
    this.stream = socket[Symbol.asyncIterator]();
    this.buffer = Buffer.alloc(0);
    this.fromMoon = 0;
    this.toMoon = 0;
    this.channel = 7;
  }

  async take(count) {
    while (this.buffer.length < count) {
      const chunk = await this.stream.next();
      assert.ok(!chunk.done, 'unexpected AMQP transport EOF');
      this.buffer = Buffer.concat([this.buffer, chunk.value]);
    }
    const bytes = this.buffer.subarray(0, count);
    this.buffer = this.buffer.subarray(count);
    return bytes;
  }

  async read(code) {
    const sizeField = await this.take(4);
    const size = sizeField.readUInt32BE(0);
    assert.ok(size >= 8 && size <= 620);
    const wire = Buffer.concat([sizeField, await this.take(size - 4)]);
    const parsed = frames.read_frame(wire);
    assert.equal(parsed.performative.constructor.descriptor.numeric, code);
    const fields = new types.Reader(wire.subarray(wire[4] * 4)).read().value;
    if (code === 0x14) this.fromMoon++;
    return { perf: parsed.performative, channel: parsed.channel, payload: parsed.payload, fields };
  }

  send(code, fields, payload, channel = this.channel) {
    const wire = frames.write_frame(frames.amqp_frame(channel, described(code, fields), payload));
    assert.ok(wire.length <= 620);
    if (code === 0x14) this.toMoon++;
    this.socket.write(wire);
  }

  async open() {
    assert.deepEqual(await this.take(8), header);
    const open = await this.read(0x10);
    assert.equal(open.perf.container_id, 'moon-recovery-probe');
    this.socket.write(header);
    this.send(0x10, [text('rhea-recovery-broker'), empty, uint(620)], undefined, 0);
    const begin = await this.read(0x11);
    this.send(0x11, [types.wrap_ushort(begin.channel), uint(0), uint(64), uint(64)]);
  }

  attach(request, handle, sender, deliveryCount, state = empty, incomplete = false) {
    this.send(0x12, [
      text('tasks'), uint(handle), bool(!sender), byte(2), byte(1),
      request.fields[5], request.fields[6], state, bool(incomplete),
      sender ? uint(deliveryCount) : empty,
    ]);
  }

  credit(handle, deliveryCount, count) {
    this.send(0x13, [
      uint(this.fromMoon), uint(64), uint(this.toMoon), uint(64),
      uint(handle), uint(deliveryCount), uint(count),
    ]);
  }

  transfer(handle, id, tag, payload, options = {}) {
    this.send(0x14, [
      uint(handle), uint(id), binary(tag), uint(0), bool(options.settled || false),
      bool(options.more || false), empty, options.state || empty, bool(options.resume || false),
    ], payload);
  }

  dispose(receiver, id, settled) {
    this.send(0x15, [bool(receiver), uint(id), empty, bool(settled), accepted]);
  }

  async close() {
    await this.read(0x18);
    this.send(0x18, [], undefined, 0);
    this.socket.end();
  }
}

function checkDelivery(frame, tag, id, resume, settled) {
  assert.deepEqual(frame.perf.delivery_tag, tag);
  assert.equal(frame.perf.delivery_id, id);
  assert.equal(Boolean(frame.perf.resume), resume);
  assert.equal(Boolean(frame.perf.settled), settled);
}

function checkDisposition(frame, role, id, settled) {
  assert.equal(Boolean(frame.perf.role), role);
  assert.equal(frame.perf.first, id);
  assert.equal(Boolean(frame.perf.settled), settled);
}

async function brokerReceives(first, next) {
  await first.open();
  const opened = await first.read(0x12);
  first.attach(opened, 3, false, 0);
  first.credit(3, opened.perf.initial_delivery_count, 2);
  const a = await first.read(0x14);
  checkDelivery(a, tagA, 0, false, false);
  assert.deepEqual(message.decode(a.payload).body.content, Buffer.from('done'));
  const partial = await first.read(0x14);
  checkDelivery(partial, tagB, 1, false, false);
  assert.ok(partial.perf.more && partial.payload.length > 0);
  let more = partial.perf.more;
  while (more) {
    // The service disconnects before consuming later fragments.
    more = (await first.read(0x14)).perf.more;
  }
  first.socket.end();

  const peer = await next();
  await peer.open();
  const round1 = await peer.read(0x12);
  assert.deepEqual([...entries(round1.fields[7]).keys()], [tagA.toString(), tagB.toString()]);
  peer.attach(round1, 5, false, 0, unsettled([[tagA, accepted]]), true);
  peer.credit(5, round1.perf.initial_delivery_count, 1);
  const settleA = await peer.read(0x14);
  checkDelivery(settleA, tagA, 0, true, true);
  assert.equal(settleA.payload?.length || 0, 0);
  assert.equal(number(settleA.fields[7].descriptor.value), 0x24);
  const detach = await peer.read(0x16);
  assert.equal(Boolean(detach.perf.closed), false);
  peer.send(0x16, [uint(5), bool(false)]);

  const round2 = await peer.read(0x12);
  assert.deepEqual([...entries(round2.fields[7]).keys()], [tagB.toString()]);
  assert.notEqual(round1.perf.handle, round2.perf.handle);
  peer.attach(round2, 6, false, 0, unsettled([[tagB, received(partial.payload.length)]]));
  peer.credit(6, round2.perf.initial_delivery_count, 2);
  const b = await peer.read(0x14);
  checkDelivery(b, tagB, 1, true, false);
  assert.equal(offset(b.fields[7]), partial.payload.length);
  const suffix = [b.payload];
  more = b.perf.more;
  while (more) {
    const fragment = await peer.read(0x14);
    suffix.push(fragment.payload);
    more = fragment.perf.more;
  }
  assert.deepEqual(message.decode(Buffer.concat([partial.payload, ...suffix])).body.content, body);
  peer.dispose(true, 1, false);
  checkDisposition(await peer.read(0x15), false, 1, true);
  checkDelivery(await peer.read(0x14), tagC, 2, false, true);
  await peer.close();
}

async function brokerSends(first, next) {
  const original = data(body);
  await first.open();
  const opened = await first.read(0x12);
  first.attach(opened, 3, true, 0);
  await first.read(0x13);
  first.transfer(3, 0, tagA, data(Buffer.from('done')));
  first.transfer(3, 1, tagB, original.subarray(0, 32), { more: true });
  checkDisposition(await first.read(0x15), true, 0, false);
  first.socket.end();

  const peer = await next();
  await peer.open();
  const round1 = await peer.read(0x12);
  const visible = entries(round1.fields[7]);
  assert.deepEqual([...visible.keys()], [tagA.toString()]);
  assert.ok(round1.perf.incomplete_unsettled);
  assert.equal(number(visible.get(tagA.toString()).descriptor.value), 0x24);
  peer.attach(round1, 5, true, 2, unsettled([[tagA, empty], [tagB, empty]]));
  await peer.read(0x13);
  peer.transfer(5, 0, tagA, undefined, { resume: true, settled: true, state: accepted });
  peer.send(0x16, [uint(5), bool(false)]);
  assert.equal(Boolean((await peer.read(0x16)).perf.closed), false);

  peer.attach(round1, 6, true, 3, unsettled([[tagB, empty]]));
  const round2 = await peer.read(0x12);
  const pending = entries(round2.fields[7]);
  assert.deepEqual([...pending.keys()], [tagB.toString()]);
  assert.equal(Boolean(round2.perf.incomplete_unsettled), false);
  const position = offset(pending.get(tagB.toString()));
  assert.equal(position, 32);
  await peer.read(0x13);
  const suffix = original.subarray(position);
  peer.transfer(6, 1, tagB, suffix.subarray(0, 400), {
    resume: true, more: true, state: received(position),
  });
  for (let i = 400; i < suffix.length; i += 400) {
    peer.send(0x14, [uint(6), empty, empty, empty, empty, bool(i + 400 < suffix.length)], suffix.subarray(i, i + 400));
  }
  checkDisposition(await peer.read(0x15), true, 1, false);
  peer.dispose(false, 1, true);
  peer.transfer(6, 2, tagC, data(Buffer.from('next')), { settled: true });
  await peer.close();
}

async function exercise(direction, scenario) {
  const server = net.createServer();
  const queued = [];
  const acceptedSockets = new Set();
  let waiting;
  server.on('connection', socket => {
    acceptedSockets.add(socket);
    socket.once('close', () => acceptedSockets.delete(socket));
    if (waiting) {
      const resolve = waiting;
      waiting = undefined;
      resolve(new WirePeer(socket));
    } else {
      queued.push(new WirePeer(socket));
    }
  });
  const next = () => queued.shift() || new Promise(resolve => { waiting = resolve; });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const child = spawn(executable, ['probe-recovery', String(server.address().port), direction], {
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', text => { stderr += text; });
  const exited = once(child, 'exit');
  let timer;
  try {
    await Promise.race([
      (async () => {
        await scenario(await next(), next);
        const [code] = await exited;
        assert.equal(code, 0, stderr);
      })(),
      new Promise((_, reject) => {
        exited.then(([code]) => { if (code !== 0) reject(new Error(stderr || 'probe exited')); }, reject);
      }),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('AMQP recovery probe timed out: ' + stderr)), 15000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
    child.kill();
    for (const socket of acceptedSockets) socket.destroy();
    server.close();
  }
  console.log('Independent TCP fixture completed recovery with MoonAMQP1 in ' + direction + ' role.');
}

(async () => {
  await exercise('send', brokerReceives);
  await exercise('receive', brokerSends);
})().catch(error => { console.error(error); process.exitCode = 1; });
