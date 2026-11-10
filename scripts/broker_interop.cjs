// Rhea is an independent producer/consumer, while Artemis owns both queues.
'use strict';
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const net = require('node:net');
const path = require('node:path');
const rhea = require('rhea');
const frames = require('rhea/lib/frames');

const host = '127.0.0.1';
const port = Number(process.env.MOONAMQP1_BROKER_PORT || 25672);
const executable = process.env.MOONAMQP1_EXE ||
  path.resolve(__dirname, '../_build/native/debug/build/cmd/main/main.exe');
const token = randomUUID();
const outbound = 'moon-out';
const inbound = 'moon-in';

function traceFrames(trace, direction) {
  let buffered = Buffer.alloc(0);
  let header = false;
  return (chunk) => {
    buffered = Buffer.concat([buffered, chunk]);
    if (!header && buffered.length >= 8) {
      buffered = buffered.subarray(8);
      header = true;
    }
    while (header && buffered.length >= 8) {
      const length = buffered.readUInt32BE(0);
      if (length < 8 || length > buffered.length) break;
      const frame = frames.read_frame(buffered.subarray(0, length));
      const perf = frame.performative;
      if (perf) {
        let detail = '';
        if (perf.constructor.descriptor.numeric === 0x13) {
          detail = ` credit=${perf.link_credit} count=${perf.delivery_count} drain=${perf.drain}`;
        } else if (perf.constructor.descriptor.numeric === 0x14) {
          assert.ok(length <= 512, 'transfer exceeded the configured test frame size');
          detail = ` id=${perf.delivery_id} more=${perf.more}`;
        } else if (perf.constructor.descriptor.numeric === 0x15) {
          detail = ` id=${perf.first} role=${perf.role} settled=${perf.settled}`;
        }
        trace.push(`${direction} ch=${frame.channel} ${perf.constructor.descriptor.numeric.toString(16)}${detail}`);
      }
      buffered = buffered.subarray(length);
    }
  };
}

async function run() {
  const container = rhea.create_container({ id: 'rhea-artemis-' + token });
  const connection = container.connect({ host, port, reconnect: false });
  let child;
  let socket;
  let stderr = '';
  let timeout;
  const trace = [];
  try {
    await new Promise((resolve, reject) => {
      let seeded = false;
      let acceptedSeeds = 0;
      let moonExited = false;
      let checkedOutbound = false;
      let checkingQueue = false;
      let closing = false;
      const fail = (error) => reject(error instanceof Error ? error : new Error(String(error)));
      const finish = () => {
        if (!moonExited || !checkedOutbound || checkingQueue) return;
        checkingQueue = true;
        // Verify that Artemis applied MoonBit's Accepted before the client closed.
        const probe = connection.open_receiver({
          source: { address: inbound }, credit_window: 0, autoaccept: false,
        });
        probe.on('message', () => fail(new Error('Artemis redelivered the MoonBit-accepted message')));
        probe.once('receiver_open', () => {
          probe.add_credit(1);
          probe.drain_credit();
        });
        probe.once('receiver_drained', () => {
          closing = true;
          connection.close();
        });
      };
      timeout = setTimeout(() => fail(new Error(
        `broker interop timed out (seeded=${seeded}, moonExited=${moonExited}, outbound=${checkedOutbound})\n` +
        stderr + trace.join('\n')
      )), 20000);
      for (const name of ['connection_error', 'session_error', 'sender_error', 'receiver_error']) {
        connection.on(name, (ctx) => fail(new Error(name + ': ' + JSON.stringify(ctx.error))));
      }
      connection.on('disconnected', (ctx) => {
        if (!closing) fail(ctx.error || new Error('broker disconnected'));
      });
      connection.once('connection_close', () => {
        if (closing) resolve();
        else fail(new Error('broker closed before both directions passed'));
      });
      const sender = connection.open_sender({ target: { address: inbound } });
      const receiver = connection.open_receiver({
        source: { address: outbound }, credit_window: 1, autoaccept: false,
      });
      sender.once('sendable', () => {
        for (const body of [Buffer.alloc(6000, 0xa5), Buffer.from('drain')]) {
          sender.send({
            application_properties: { token, origin: 'rhea' },
            body: rhea.message.data_section(body),
          });
        }
      });
      sender.on('accepted', () => {
        if (++acceptedSeeds !== 2) return;
        // Seed the queue before MoonBit requests a finite drain.
        seeded = true;
        child = spawn(executable, ['broker', outbound, inbound, token], {
          stdio: ['pipe', 'pipe', 'pipe'],
        });
        child.stderr.setEncoding('utf8').on('data', (chunk) => { stderr += chunk; });
        child.once('error', fail);
        child.once('close', (code) => {
          if (code !== 0) return fail(new Error('MoonBit exit ' + code + '\n' + stderr));
          moonExited = true;
          finish();
        });
        socket = net.createConnection({ host, port });
        socket.once('error', fail);
        for (const [stream, direction] of [[child.stdout, 'Moon->broker'], [socket, 'broker->Moon']]) {
          const observe = traceFrames(trace, direction);
          stream.on('data', (chunk) => {
            try { observe(chunk); } catch (error) { fail(error); }
          });
        }
        child.stdin.on('error', (error) => { if (error.code !== 'EPIPE') fail(error); });
        socket.pipe(child.stdin);
        child.stdout.pipe(socket);
      });
      receiver.on('message', ({ message, delivery }) => {
        try {
          assert.ok(seeded);
          assert.equal(checkedOutbound, false);
          assert.equal(message.application_properties.token, token);
          assert.equal(message.application_properties.origin, 'moonamqp1');
          assert.deepEqual(message.body.content, Buffer.alloc(5000, 0x42));
          delivery.accept();
          checkedOutbound = true;
          finish();
        } catch (error) {
          fail(error);
        }
      });
    });
  } finally {
    clearTimeout(timeout);
    socket?.destroy();
    child?.kill();
    connection.close();
    // An error or unresponsive broker must not keep this process alive.
    connection.socket?.destroy();
  }
  const sent = trace.filter((line) => /^Moon->broker ch=\d+ 14 /.test(line)).length;
  const received = trace.filter((line) => /^broker->Moon ch=\d+ 14 /.test(line)).length;
  assert.ok(sent > 1 && received > 1, trace.join('\n'));
  console.log('PASS MoonAMQP1 -> Artemis -> Rhea: 5000 bytes, typed properties, Accepted');
  console.log('PASS Rhea -> Artemis -> MoonAMQP1: 6000-byte and 5-byte messages, typed properties, Accepted, no redelivery');
  console.log(`PASS one connection, two sessions, 512-byte frame limit, ${sent}/${received} transfer frames`);
  console.log('PASS drain: one queued message plus 2 returned credits; empty queue returned all 2 credits');
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
