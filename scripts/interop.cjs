// Original synthetic vectors; Rhea is an independent Apache-2.0 development oracle.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const cp = require('node:child_process');
const { Duplex } = require('node:stream');
const rhea = require('rhea');
const types = require('rhea/lib/types.js');
const frames = require('rhea/lib/frames.js');
const message = require('rhea/lib/message.js');
const root = path.resolve(__dirname, '..');
const exe = process.env.MOONAMQP1_EXE || path.join(root, '_build/native/debug/build/cmd/main', process.platform === 'win32' ? 'main.exe' : 'main.exe');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'moonamqp1-'));
let counter = 0;
function run(mode, bytes) {
  const input = path.join(temp, `in-${counter}.bin`);
  const output = path.join(temp, `out-${counter++}.bin`);
  fs.writeFileSync(input, bytes);
  const result = cp.spawnSync(exe, [mode, input, output], { encoding: 'utf8', timeout: 15000 });
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return fs.readFileSync(output);
}
function encode(value) { const writer = new types.Writer(); writer.write(value); return writer.toBuffer(); }
function normalizeCode(code) {
  return ({ 0x41:0x56,0x42:0x56,0x43:0x70,0x44:0x80,0x45:0xd0,0x52:0x70,0x53:0x80,0x54:0x71,0x55:0x81,0xa0:0xb0,0xa1:0xb1,0xa3:0xb3,0xc0:0xd0,0xc1:0xd1,0xe0:0xf0 })[code] || code;
}
function signature(value) {
  if (Buffer.isBuffer(value)) return { hex: value.toString('hex') };
  if (Array.isArray(value)) return value.map(signature);
  if (value && value.type) {
    const code = normalizeCode(value.type.typecode);
    let data = value.value;
    if (code === 0x56) data = Boolean(data);
    if ((code === 0x80 || code === 0x81) && !Buffer.isBuffer(data)) {
      const bytes = Buffer.alloc(8);
      if (code === 0x80) bytes.writeBigUInt64BE(BigInt(data)); else bytes.writeBigInt64BE(BigInt(data));
      data = bytes;
    }
    return { code, data: signature(data), descriptor: value.descriptor ? signature(value.descriptor) : undefined,
      array: value.array_constructor ? { code: normalizeCode(value.array_constructor.typecode), descriptor: value.array_constructor.descriptor ? signature(value.array_constructor.descriptor) : undefined } : undefined };
  }
  return value;
}
function read(bytes) { const reader = new types.Reader(bytes); const value = reader.read(); assert.equal(reader.position, bytes.length); return value; }
function codecs() {
  const vectors = [
    types.Null(), types.wrap_boolean(true), types.wrap_boolean(false),
    types.wrap_ubyte(255), types.wrap_ushort(65535), types.wrap_uint(0), types.wrap_uint(4294967295),
    types.wrap_ulong(Buffer.from('ffffffffffffffff','hex')), types.wrap_long(Buffer.from('8000000000000000','hex')),
    types.wrap_byte(-128), types.wrap_short(-32768), types.wrap_int(-2147483648),
    types.wrap_float(-0), types.wrap_double(1.25), types.wrap_char(0x1f680), types.wrap_timestamp(1234567890123),
    types.wrap_uuid(Buffer.from('00112233445566778899aabbccddeeff','hex')),
    types.wrap_binary(Buffer.from([0, 255, 17])), types.wrap_string('任务 MoonBit 🌙'), types.wrap_symbol('amqp:example'),
    types.wrap_list([types.wrap_ulong(256), 'name', false, [1,2]]),
    types.wrap_map({ unit:'C', sequence:types.wrap_ulong(Buffer.from('ffffffffffffffff','hex')) }),
    types.wrap_array([1,2,4294967295], 0x70), types.wrap_array([], 0xb3),
    types.described(types.wrap_ulong(0x24), types.wrap_list([])),
    types.described(types.wrap_symbol('example:typed'), types.wrap_list(['hello'])),
    types.wrap_array([[],[]], 0xd0, types.wrap_ulong(0x24)),
  ];
  for (const vector of vectors) {
    const original = encode(vector);
    const moon = run('value', original);
    assert.deepEqual(signature(read(moon)), signature(read(original)));
    assert.deepEqual(run('value', encode(read(moon))), moon);
  }
  const msg = { durable:true, priority:7, ttl:30000, message_id:types.wrap_ulong(Buffer.from('ffffffffffffffff','hex')),
    to:'jobs', subject:'asset', content_type:'application/octet-stream', creation_time:1234567890123,
    application_properties:{ sequence:types.wrap_ulong(Buffer.from('ffffffffffffffff','hex')), unit:'C' },
    message_annotations:{ 'x-origin':'original' }, body:message.data_sections([Buffer.from('one'),Buffer.from('two')]) };
  const original = message.encode(msg);
  const moon = run('message', original);
  const decoded = message.decode(moon);
  assert.equal(decoded.to, 'jobs');
  assert.equal(decoded.subject, 'asset');
  assert.deepEqual(decoded.message_id, Buffer.from('ffffffffffffffff','hex'));
  const before = new types.Reader(original), after = new types.Reader(moon), rewritten = new types.Writer();
  while (before.position < original.length) {
    const a = before.read(), b = after.read();
    assert.deepEqual(signature(a), signature(b));
    rewritten.write(b);
  }
  assert.equal(after.position, moon.length);
  assert.deepEqual(run('message', rewritten.toBuffer()), moon);
  const input = Buffer.concat([Buffer.from('414d515000010000','hex'), ...[
    frames.amqp_frame(0, frames.open({container_id:'rhea', max_frame_size:512})),
    frames.amqp_frame(11, frames.begin({next_outgoing_id:0,incoming_window:32,outgoing_window:32})),
    frames.amqp_frame(11, frames.attach({name:'work',handle:37,role:false,initial_delivery_count:0,target:types.described(types.wrap_ulong(0x29),types.wrap_list(['jobs']))})),
    frames.amqp_frame(11, frames.transfer({handle:37,delivery_id:0,delivery_tag:Buffer.from('one'),message_format:0}), original),
    frames.amqp_frame(0, frames.close({})),
  ].map(frames.write_frame)]);
  const canonical = run('frames', input);
  assert.deepEqual(run('frames', canonical), canonical);
  let pos = 8, frameCount = 0;
  while (pos < canonical.length) { const frame = frames.read_frame(canonical.subarray(pos)); assert(frame); pos += frame.size; frameCount++; }
  assert.equal(frameCount,5);
  console.log(`Rhea 3.0.5: ${vectors.length} typed values, message sections and frame streams passed bidirectional checks`);
}
async function livePeer() {
  const child = cp.spawn(exe, ['peer'], { stdio:['pipe','pipe','pipe'] });
  let stderr = '';
  child.stderr.on('data', b => { stderr += b; });
  child.on('exit', code => { if (code !== 0) console.error(`Moon peer exit ${code}: ${stderr}`); });
  const socket = Duplex.from({ readable:child.stdout, writable:child.stdin });
  const container = rhea.create_container({ id:'rhea-oracle' });
  let count = 0, delivery;
  const closed = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error(`live peer timed out: ${stderr}`)), 12000);
    container.on('connection_error', context => reject(Error(JSON.stringify(context.connection.error))));
    container.on('disconnected', context => { if (context.error) reject(context.error); });
    container.on('sendable', context => {
      if (delivery) return;
      delivery = context.sender.send({ to:'jobs', application_properties:{ id:types.wrap_ulong(Buffer.from('ffffffffffffffff','hex')) }, body:message.data_section(Buffer.alloc(5000,0x31)) });
    });
    container.on('accepted', context => { count++; assert.equal(context.delivery.id, delivery.id); context.connection.close(); });
    container.on('connection_close', () => { clearTimeout(timer); resolve(); });
  });
  try {
    const connection = container.connect({ reconnect:false, max_frame_size:512, idle_time_out:0,
      connect:(_port,_host,_options,connected) => { process.nextTick(connected); return socket; } });
    connection.open_sender({ name:'oracle-task', target:{ address:'jobs' }, snd_settle_mode:2, rcv_settle_mode:0 });
    await closed;
    assert.equal(count,1);
    child.stdin.end();
    const exitCode = await new Promise(resolve => child.on('exit',resolve));
    assert.equal(exitCode,0,stderr);
    console.log('Live Rhea client over binary stdio: open/begin/attach/credit/fragmented transfer/accepted/close passed');
  } finally { child.kill(); }
}
(async () => { try { codecs(); await livePeer(); } finally { fs.rmSync(temp,{recursive:true,force:true}); } })().catch(error => { console.error(error); process.exitCode=1; });
