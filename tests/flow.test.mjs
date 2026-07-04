// Session-flow simulation for the embedded client app.
//
// Run from the repo root:
//   npm install --no-save jsdom jsqr fake-indexeddb
//   node tests/flow.test.mjs
// (install all three in ONE command — npm --no-save prunes other --no-save pkgs)
//
// Renders the page through the real Worker fetch handler, boots it in jsdom
// with mocked WebSocket / fetch / audio graph / clipboard, and drives:
//   1. happy path: buffer-while-connecting, tail streaming, commit, await-final
//   2. unexpected mid-dictation disconnect (must fail loudly, keep partial text)
//   3. dead-mic flatline alarm + empty-session sentinel
//   4. append mode appends consecutive recordings (no time window)
//   5. connect timeout fails loudly with sentinel
//   6. PTT pressed during finalization queues a new session
//   7. configurable hotkey: tap toggles, hold is push-to-talk
//   8. engine selector: per-mode controls + persistence
//   9. batch engine happy path (no socket; upload -> clipboard)
//  10. batch upload failure -> sentinel, loud status
//  11. PTT queued during a slow batch upload
//  12. hybrid happy path: live feedback, refined text on the clipboard
//  13. hybrid refine failure: live text delivered with a warn
//  14. hybrid recovery: WS dies mid-dictation, batch refine still delivers
//  15. hybrid append parity: previous_text on the first frame, refined splice
//  16. hybrid with zero live text: refine still delivers
//  17. click-to-append: clicking the transcript box arms a one-shot append
//  18. keyterm presets: injected lists render as checkboxes, merge into both
//      APIs (custom > checked presets > always-on), dedupe, persist
//  19. phone mic session: desktop listener mirrors transcripts + delivers
//      phone_delivery to the clipboard; phone rides the session code on its
//      WS and POSTs the final text to the room
//  20. phone link resilience: session survives phone_session_end, dropped
//      listener socket reconnects loudly, replayed deliveries dedupe by id,
//      unfocused clipboard copy retries on refocus, session_end grace window
//      falls back to live text (cancelled by the real delivery), and the
//      phone treats a zero-listener deliver ack as a loud failure
//  20q. phone-side durable delivery queue: a failed/zero-listener /deliver
//      enqueues the text (persisted + CODE-BOUND), an online/boot heal re-POSTs
//      it with the SAME id until a listener acks (then the queue clears), the
//      desktop ring-dedupe drops a stale re-POST arriving after a newer
//      delivery, Leave KEEPS the queue (rejoining the same code auto-delivers —
//      the stranded-note incident self-heals), a different-code join keeps the
//      item but never POSTs it there, and a legacy no-code item is stamped with
//      the persisted join at boot
//  34. manual "Send to desktop" (box / history rows / expanded big-peek, joined
//      + idle only) pushes existing text through the normal queue+cue path with
//      a fresh delivery_id; the queue chip surfaces undelivered notes and a tap
//      retries; everything hides when unjoined
//  21. SessionRoom DO contract (direct): ping/pong, listener-count ack,
//      held-delivery replay inside the window only, transcripts not buffered,
//      GET /latest for native pollers (fresh delivery vs stale null)
//  22. phone link persistence: desktop session + phone join survive reloads
//      (resume room at boot, replay deduped by the persisted delivery id,
//      Leave/End forget the stored codes)
//  23. iOS mic resilience: screen wake lock held per dictation, muted-track
//      rebuild (iOS interruptions leave tracks "live" but muted), and the
//      visibility re-warm working without the Permissions API (iOS Safari)
//  24. QR join: desktop renders a locally-encoded QR of /?join=<code> that a
//      real decoder (jsqr) reads back; opening that URL on the phone joins,
//      persists, and cleans the address bar
//  25. big-button dictation layout: joining flips it on and Leave reverts,
//      a persisted join and the /?join= boot path land straight in it, the
//      button drives the normal session paths with hotkey tap/hold semantics
//      (slide-away via the document backstop, sub-threshold pointercancel
//      stops, multi-touch ignored, a queued press cancelled/F14'd/released-
//      after-delivery never auto-starts an unheld mic), the whole-screen
//      state mirrors status/pill transitions (zero-listener relay ack and
//      relay failures redden it — even with a queued tap pending; the
//      finalize gap renders WORKING, never a stale success; the no-speech
//      sentinel outcome reads FAILED), haptics mirror the beep patterns,
//      the peek strip expands + click-to-append works, the normal settings
//      stay reachable, the per-device override persists ("never" wins over
//      a join), and a joined phone whose local clipboard is denied (iOS: no
//      write outside a gesture) defers the outcome to the relay ack instead
//      of a false copy-FAILED — while zero-listener/relay-failure/unjoined
//      outcomes stay loud failures
// (scenario 0, asserted right after boot: legacy access-code migration shim,
//  batch default engine, append-mode off by default, latest transcript
//  restored from history, and the auth section's open/collapse behavior)
//  29. batch-only product: saved Hybrid->Batch migration, no engine selector,
//      live capture feedback shows/hides, batch text reaches the clipboard
//  30. diarization keep-primary-speaker: default-on toggle sends diarize=true, a
//      two-speaker words[] drops the bystander with a no-beep "removed N words"
//      status note, a single speaker is a no-op, and toggling off sends
//      diarize=false + skips client filtering + persists per-device
//  25w. joined + degraded outcome: a large speaker-filter cut delivered over the
//      relay gets warnBeep + VERIFY on the delivered ack — never doneBeep
//  23p. phone corpse-mic probe (every press): a dead analyser read forces ONE
//      pre-capture rebuild and a still-dead mic fails LOUD before capture (no
//      REC, no upload, sentinel); a healthy mic exits on frame 0 (no rebuild)
//  37. fresh-graph silent capture (the VPIO wall): a FRESH getUserMedia stream
//      that delivers silence gets one rebuild then a loud pre-capture failure
//  38. idle mic-health sampler: two consecutive dead idle frames proactively
//      rebuild the retained graph (never mid-recording) + the Mic ✓/⚠ pill
//  39. transcript-coverage guard: a result whose last word ends far short of the
//      speech the gate observed (or whose decoded audio is far shorter than the
//      recording) is a degraded WARN (text still delivered + diag ring entry),
//      a full-coverage take stays a clean Done!, words without timestamps and
//      absent duration are clean no-ops, and the gate-open baseline kills the
//      held-but-silent false warn
//  40. duration-aware upload deadline: a long take's transcription outlives the
//      old flat 15 s deadline and still succeeds
//  41. journal cap honesty: a recovery capped at JOURNAL_MAX_CHUNKS says only
//      the first ~N minutes were saved; an uncapped one doesn't
//
// Exits non-zero on any failure. Extend these scenarios whenever the session
// flow, beeps, clipboard behavior, or watchdog change.

import { JSDOM } from 'jsdom';
import jsQR from 'jsqr';
import { IDBFactory } from 'fake-indexeddb';
import { dirname, join } from 'path';
import { fileURLToPath, pathToFileURL } from 'url';

const here = dirname(fileURLToPath(import.meta.url));
const worker = await import(pathToFileURL(join(here, '..', 'worker.js')).href);
const res = await worker.default.fetch(new Request('https://dictation.test/'), {});
const html = await res.text();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let failures = 0;
function check(name, cond, extra) {
  console.log((cond ? 'PASS' : 'FAIL') + ' - ' + name + (extra !== undefined ? '  [' + extra + ']' : ''));
  if (!cond) failures++;
}

// ---- mocks ----
const sockets = [];
class MockWS {
  constructor(url) {
    this.url = url;
    this.readyState = 0; // CONNECTING
    this.sent = [];
    this.closed = false;
    sockets.push(this);
  }
  send(d) { if (this.readyState !== 1) throw new Error('not open'); this.sent.push(d); }
  close() { if (this.closed) return; this.closed = true; this.readyState = 3; if (this.onclose) this.onclose({}); }
  // test helpers
  open() { this.readyState = 1; if (this.onopen) this.onopen({}); }
  msg(obj) { if (this.onmessage) this.onmessage({ data: JSON.stringify(obj) }); }
  serverClose() { this.readyState = 3; const wasClosed = this.closed; this.closed = true; if (!wasClosed && this.onclose) this.onclose({}); }
}
MockWS.CONNECTING = 0; MockWS.OPEN = 1; MockWS.CLOSING = 2; MockWS.CLOSED = 3;

let micRms = 0.05; // pretend speech level; the gate-meter watchdog reads it

class MockAudioCtx {
  constructor() { this.state = 'running'; this.currentTime = 0; this.sampleRate = 48000; this.destination = {}; }
  resume() { return Promise.resolve(); }
  close() { return Promise.resolve(); }
  createMediaStreamSource() { return { connect() {} }; }
  createBiquadFilter() { return { type: '', frequency: { value: 0 }, Q: { value: 0 }, connect() {} }; }
  createAnalyser() {
    return {
      fftSize: 1024,
      connect() {},
      getFloatTimeDomainData(buf) { buf.fill(micRms); },
    };
  }
  createGain() { return { gain: { value: 0, setTargetAtTime() {} }, connect() {} }; }
  createMediaStreamDestination() { return { stream: { tag: 'dest' }, connect() {} }; }
  createOscillator() { return { frequency: { value: 0 }, connect() {}, start() {}, stop() {} }; }
}

const micTrack = { readyState: 'live', muted: false, listeners: {}, addEventListener(ev, fn) { this.listeners[ev] = fn; }, stop() { this.readyState = 'ended'; } };
const mockStream = { getAudioTracks: () => [micTrack], getTracks: () => [micTrack] };

let clipboard = '';
let gumCalls = 0;        // getUserMedia acquisitions (mic re-engagement paths)
let lastGumConstraints = null; // the constraints object of the most recent getUserMedia
let wakeLockCalls = 0;   // wake lock acquisitions
let activeWakeLock = null;

// Queue-driven fetch mock for the batch upload path. Each entry:
// { delayMs?, status, body }. An empty queue answers 500 so an unexpected
// upload fails the scenario loudly instead of hanging.
const fetchQueue = [];
const fetchCalls = [];
const mainCtxInstances = []; // only the MAIN DOM's AudioContexts (the app's live one is last)

const dom = new JSDOM(html, {
  runScripts: 'dangerously',
  url: 'https://dictation.test/',
  beforeParse(window) {
    window.AudioContext = class extends MockAudioCtx { constructor() { super(); mainCtxInstances.push(this); } };
    window.WebSocket = MockWS;
    window.MediaRecorder = class {
      constructor(stream, opts) { this.state = 'inactive'; this.stream = stream; this.opts = opts; }
      static isTypeSupported() { return false; }
      start() { this.state = 'recording'; }
      stop() {
        if (this.state === 'inactive') return;
        this.state = 'inactive';
        // Deliver a plausible recording so size gates and the preview path run.
        if (this.ondataavailable) {
          this.ondataavailable({ data: new window.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) });
        }
        if (this.onstop) this.onstop();
      }
    };
    window.URL.createObjectURL = () => 'blob:mock';
    window.URL.revokeObjectURL = () => {};
    window.fetch = (url, opts) => {
      fetchCalls.push({ url: String(url), opts: opts || {}, form: opts && opts.body });
      const next = fetchQueue.shift() || { status: 500, body: { error: 'unexpected fetch (queue empty)' } };
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => {
          resolve({
            ok: next.status >= 200 && next.status < 300,
            status: next.status,
            text: () => Promise.resolve(JSON.stringify(next.body)),
          });
        }, next.delayMs || 5);
        if (opts && opts.signal) {
          opts.signal.addEventListener('abort', () => {
            clearTimeout(t);
            const err = new Error('aborted');
            err.name = 'AbortError';
            reject(err);
          });
        }
      });
    };
    Object.defineProperty(window.navigator, 'mediaDevices', {
      // A fresh getUserMedia always yields a LIVE track (the real API does), so a
      // re-acquisition after releaseAudio (e.g. the big-button corpse-mic guard)
      // revives the singleton track instead of handing back a stale "ended" one.
      // The constraints are captured so a rebuild's echoCancellation/autoGainControl
      // can be asserted (scenario 31m).
      value: { getUserMedia: (c) => { gumCalls++; lastGumConstraints = c; micTrack.readyState = 'live'; return Promise.resolve(mockStream); }, addEventListener() {} },
      configurable: true,
    });
    Object.defineProperty(window.navigator, 'wakeLock', {
      value: { request: () => { wakeLockCalls++; activeWakeLock = { release() { activeWakeLock = null; return Promise.resolve(); } }; return Promise.resolve(activeWakeLock); } },
      configurable: true,
    });
    Object.defineProperty(window.navigator, 'permissions', {
      value: { query: () => Promise.resolve({ state: 'granted' }) }, // warm mic on load
      configurable: true,
    });
    Object.defineProperty(window.navigator, 'clipboard', {
      value: { writeText(t) { clipboard = t; return Promise.resolve(); } },
      configurable: true,
    });
    Object.defineProperty(window, 'isSecureContext', { value: true, configurable: true });
    // Scenario 0 seed: a pre-merge batch-app user with a remembered access
    // code under the legacy key. The page must surface it as the passphrase.
    window.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ saveApiKey: true }));
    window.localStorage.setItem('scribe_v2_access_code_v9', 'legacy-code');
    // Scenario 0 seed: a saved transcript — the boot must restore it into the
    // latest-transcript box (most recent note visible after a reload).
    window.localStorage.setItem('scribe_v2_transcripts_v9', JSON.stringify([
      { text: 'Restored note. ', createdAt: new Date().toISOString(), engine: 'batch' },
    ]));
    window.addEventListener('error', (e) => { console.log('PAGE ERROR:', e.message); failures++; });
  },
});

const w = dom.window;
const doc = w.document;
const status = () => doc.getElementById('status').textContent.trim();
const statusCls = () => doc.getElementById('status').className;
const latest = () => doc.getElementById('latest').textContent;

await sleep(200);

// ===== Scenario 0: boot state — migration shim + defaults + restore =====
console.log('--- scenario 0: boot migration + defaults + restore ---');
check('legacy access code surfaced as passphrase', doc.getElementById('passphrase').value === 'legacy-code', JSON.stringify(doc.getElementById('passphrase').value));
check('append mode unchecked by default', !doc.getElementById('appendMode').checked);
check('legacy clean-up options removed from the DOM (now permanently on)',
  !doc.getElementById('noVerbatim') && !doc.getElementById('stripNewlines') && !doc.getElementById('stripEllipses') && !doc.getElementById('trailingSpace'),
  [doc.getElementById('noVerbatim'), doc.getElementById('stripNewlines'), doc.getElementById('stripEllipses'), doc.getElementById('trailingSpace')].map(Boolean).join(','));
check('latest transcript restored from history on boot', latest().includes('Restored note.'), latest());
check('auth section open while credentials are missing', doc.getElementById('authSection').open === true);
check('auth summary prompts for the key', doc.getElementById('authSummary').textContent.includes('enter'), doc.getElementById('authSummary').textContent);

doc.getElementById('apiKey').value = 'test-key';
doc.getElementById('sonioxKey').value = 'test-skey';
doc.getElementById('apiKey').dispatchEvent(new w.Event('change', { bubbles: true }));
check('auth section collapses once a key is entered', doc.getElementById('authSection').open === false);
check('auth summary shows the key is set', doc.getElementById('authSummary').textContent.includes('✓'), doc.getElementById('authSummary').textContent);

// Turn append mode on for the append scenario (scenario 4); scenario 17
// covers the append-off default.
doc.getElementById('appendMode').click();
check('append mode toggled on for the append scenario', doc.getElementById('appendMode').checked);
doc.getElementById('freshBtn').click(); // clear the restored note so the next scenario starts fresh

// ===== Scenario 3 (batch): dead mic flatline alarm =====
// The mic watchdog runs in batch mode too (no WS dependency): a flatline signal
// fires the alarm, and a no-text finalize copies the sentinel.
console.log('--- scenario 3: batch dead mic alarm ---');
doc.getElementById('freshBtn').click();
micRms = 0.0; // flatline
doc.getElementById('recordBtn').click();
await sleep(2900); // > 2.5s flatline detection
check('mic alarm fired in batch mode', status().includes('MIC NOT CAPTURING'), status());
check('mic pill FAIL', doc.getElementById('micPill').textContent === 'MIC FAIL');
// A flatline recording still uploads; the empty transcript + the fired mic alarm
// drive the no-speech sentinel path.
fetchQueue.push({ status: 200, body: { text: '' } });
doc.getElementById('recordBtn').click(); // stop -> upload empty -> sentinel
await sleep(300);
check('dead-mic finalize -> sentinel', clipboard === '##DICTATION_FAILED##', JSON.stringify(clipboard));
check('no-speech status mentions mic', status().includes('microphone never produced a signal'), status());
micRms = 0.05;

// ===== Scenario 3b (batch): LOW MIC LEVEL fails LOUD (not silently) =====
// A real-but-quiet voice whose peak sits in the dead-band ABOVE the flatline
// floor (0.0008) but BELOW the gate (0.030) records near-empty WITHOUT tripping
// the dead-mic alarm. Pass 1 reclassifies that empty outcome as a specific
// "VERY LOW MIC LEVEL" failure (still sentinel + fail beep, exactly one
// delivery) so low gain can never masquerade as a silent capture bug. Strictly
// disjoint from the flatline alarm (which owns maxRmsSeen < FLATLINE_RMS).
console.log('--- scenario 3b: low-mic-level loud failure ---');
doc.getElementById('freshBtn').click();
micRms = 0.015; // dead-band: > FLATLINE_RMS 0.0008, < gateOpen 0.030
doc.getElementById('recordBtn').click();
await sleep(200); // let the gate-meter loop record the dead-band peak (no flatline: peak > 0.0008)
check('s3b: no flatline alarm fires in the dead-band', !status().includes('MIC NOT CAPTURING'), status());
clipboard = ''; // isolate this take's clipboard outcome from scenario 3's sentinel
fetchQueue.push({ status: 200, body: { text: '' } });
doc.getElementById('recordBtn').click(); // stop -> upload empty -> low-level classification
await sleep(300);
check('s3b: low level fails LOUD naming the cause + lever', status().includes('VERY LOW MIC LEVEL'), status());
check('s3b: low level still copies the sentinel', clipboard === '##DICTATION_FAILED##', JSON.stringify(clipboard));
check('s3b: not misreported as a dead mic', !status().includes('microphone never produced a signal'), status());
micRms = 0.05;
doc.getElementById('freshBtn').click();

// ===== Scenario 3c (batch): a TRUE-ZERO capture fails as "MIC PRODUCED NO SIGNAL" =====
// A dead / iOS-corpse mic delivers literal silence (peak 0). On a HELD take the
// 2.5s watchdog names it; on a SHORT press it slips past the watchdog and used to
// read as the mild generic "No speech detected" — blaming the clinician for not
// speaking. It now fails LOUD naming the mic (peak < FLATLINE_RMS), distinct from
// the dead-band "VERY LOW MIC LEVEL" (peak in [FLATLINE_RMS, gateOpen)).
console.log('--- scenario 3c: true-zero capture loud failure ---');
doc.getElementById('freshBtn').click();
micRms = 0.0; // true zero — the mic delivers nothing
doc.getElementById('recordBtn').click();
await sleep(200); // SHORT hold: under the 2.5s flatline watchdog, so it must classify at finalize
check('s3c: no flatline alarm on a short zero take', !status().includes('MIC NOT CAPTURING'), status());
clipboard = ''; // isolate this take's clipboard outcome
fetchQueue.push({ status: 200, body: { text: '' } });
doc.getElementById('recordBtn').click(); // stop -> upload empty -> zero-capture classification
await sleep(300);
check('s3c: a true-zero capture fails LOUD naming the mic', status().includes('MIC PRODUCED NO SIGNAL'), status());
check('s3c: true-zero capture copies the sentinel', clipboard === '##DICTATION_FAILED##', JSON.stringify(clipboard));
check('s3c: not blamed on the clinician ("no speech")', !status().includes('No speech detected'), status());
micRms = 0.05;
doc.getElementById('freshBtn').click();

// ===== Scenario 4 (batch): append mode appends consecutive recordings =====
// No time window anymore: with append mode on, the second dictation always
// extends the first regardless of how much time passed between them.
console.log('--- scenario 4: batch append mode (no time window) ---');
fetchQueue.push({ status: 200, body: { text: 'First note.' } });
doc.getElementById('recordBtn').click();
await sleep(80);
doc.getElementById('recordBtn').click();
await sleep(300);
check('first note saved', latest().includes('First note.'), latest());
await sleep(1300); // a long pause must NOT change the outcome (no window)
fetchQueue.push({ status: 200, body: { text: 'Second note.' } });
doc.getElementById('recordBtn').click();
await sleep(80);
doc.getElementById('recordBtn').click();
await sleep(300);
check('append mode keeps both notes after a long pause', latest().includes('First note.') && latest().includes('Second note.'), latest());
doc.getElementById('appendMode').click(); // back to default (off) for the remaining scenarios
doc.getElementById('freshBtn').click();

// ===== Scenario 7 (batch): configurable hotkey (default Ctrl+Space) + F13/F14 =====
console.log('--- scenario 7: batch hotkey tap + hold ---');
check('idle before hotkey tests', doc.getElementById('recordBtn').textContent.includes('Start'));
const kd = (init) => doc.dispatchEvent(new w.KeyboardEvent('keydown', init));
const ku = (init) => doc.dispatchEvent(new w.KeyboardEvent('keyup', init));

// tap = toggle on
kd({ code: 'Space', ctrlKey: true });
await sleep(80);
ku({ code: 'Space', ctrlKey: true }); // released quickly -> tap
await sleep(80);
check('hotkey tap started recording', doc.getElementById('recordBtn').textContent.includes('Stop'));
fetchQueue.push({ status: 200, body: { text: 'Hotkey tap note.' } });
// tap again = toggle off
kd({ code: 'Space', ctrlKey: true });
ku({ code: 'Space', ctrlKey: true });
await sleep(300);
check('hotkey tap-off saved + copied', clipboard.includes('Hotkey tap note.'), JSON.stringify(clipboard));

// hold = push-to-talk
kd({ code: 'Space', ctrlKey: true });
await sleep(600); // > HOTKEY_TAP_MS
check('hotkey hold started recording', doc.getElementById('recordBtn').textContent.includes('Stop'));
fetchQueue.push({ status: 200, body: { text: 'Hotkey held note.' } });
ku({ code: 'Space', ctrlKey: true }); // release after holding -> stop
await sleep(300);
check('hotkey hold note saved + copied', clipboard.includes('Hotkey held note.'), JSON.stringify(clipboard));
check('plain Space does nothing', (() => { kd({ code: 'Space' }); return doc.getElementById('recordBtn').textContent.includes('Start'); })());

// F13/F14 contract: F13 keydown starts, F14 keydown stops.
fetchQueue.push({ status: 200, body: { text: 'F13 note.' } });
doc.dispatchEvent(new w.KeyboardEvent('keydown', { code: 'F13' }));
await sleep(80);
check('F13 keydown starts recording', doc.getElementById('recordBtn').textContent.includes('Stop'));
doc.dispatchEvent(new w.KeyboardEvent('keydown', { code: 'F14' }));
await sleep(300);
check('F14 keydown stops + delivers', clipboard.includes('F13 note.'), JSON.stringify(clipboard));
doc.getElementById('freshBtn').click();

// ===== Scenario 9: batch engine happy path =====
console.log('--- scenario 9: batch happy path ---');
doc.getElementById('freshBtn').click();
const sCountBatch = sockets.length;
const fCountBatch = fetchCalls.length;
fetchQueue.push({ status: 200, body: { text: 'Batch note dictated.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
check('batch recording started', doc.getElementById('recordBtn').textContent.includes('Stop'));
check('batch status mentions upload-on-release', status().includes('release to upload'), status());
doc.getElementById('recordBtn').click(); // stop -> recorder flush -> upload
await sleep(300);
check('no WebSocket opened in batch mode', sockets.length === sCountBatch, sockets.length - sCountBatch);
check('exactly one upload', fetchCalls.length === fCountBatch + 1, fetchCalls.length - fCountBatch);
const upload = fetchCalls[fetchCalls.length - 1];
check('upload went to /api/transcribe via POST', upload.url.includes('/api/transcribe') && upload.opts.method === 'POST');
check('upload form carries api key + keyterms + tag_audio_events + file',
  upload.form && upload.form.get('api_key') === 'test-key' &&
  typeof upload.form.get('keyterms_json') === 'string' &&
  upload.form.get('tag_audio_events') === 'false' &&
  upload.form.get('file') !== null);
check('no_verbatim is always sent true (the filler-words toggle was removed)', upload.form.get('no_verbatim') === 'true', upload.form.get('no_verbatim'));
check('batch text displayed', latest().includes('Batch note dictated.'), latest());
check('batch text on clipboard', clipboard.includes('Batch note dictated.'), JSON.stringify(clipboard));
check('batch success status', status().includes('Done!'), status());
const hist9 = JSON.parse(w.localStorage.getItem('scribe_v2_transcripts_v9'));
check('history entry tagged engine batch', hist9[0] && hist9[0].engine === 'batch', hist9[0] && hist9[0].engine);
check('record button reset after batch', doc.getElementById('recordBtn').textContent.includes('Start'));
check('link pill idle after batch', doc.getElementById('linkPill').textContent === 'link idle');

// ===== Scenario 10: batch upload failure -> sentinel =====
console.log('--- scenario 10: batch upload failure ---');
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 500, body: { error: 'service exploded' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('upload failure -> sentinel on clipboard', clipboard === '##DICTATION_FAILED##', JSON.stringify(clipboard));
check('upload failure -> loud err status', statusCls().includes('err') && status().includes('FAILED'), status());
check('upload failure surfaces the server error', status().includes('service exploded'), status());
check('link pill FAIL after failed upload', doc.getElementById('linkPill').textContent === 'LINK FAIL');
check('record button reset after failure', doc.getElementById('recordBtn').textContent.includes('Start'));

// ===== Scenario 11: PTT queued during a slow batch upload =====
console.log('--- scenario 11: batch queued PTT ---');
doc.getElementById('freshBtn').click();
fetchQueue.push({ delayMs: 800, status: 200, body: { text: 'Slow upload note.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click(); // stop -> slow upload starts
await sleep(120);
doc.dispatchEvent(new w.KeyboardEvent('keydown', { code: 'F13' })); // PTT during upload
check('still uploading when PTT queued', doc.getElementById('linkPill').textContent === 'uploading…', doc.getElementById('linkPill').textContent);
await sleep(1100); // upload resolves, queued session starts (~60ms later)
check('slow upload delivered', clipboard.includes('Slow upload note.'), JSON.stringify(clipboard));
check('queued PTT started the next batch session', doc.getElementById('recordBtn').textContent.includes('Stop'));
fetchQueue.push({ status: 200, body: { text: 'Second.' } });
doc.getElementById('recordBtn').click(); // wrap up the queued session
await sleep(300);
check('queued session delivered too', clipboard.includes('Second.'), JSON.stringify(clipboard));

// ===== Scenario 17: click-to-append — clicking the box arms a one-shot append =====
console.log('--- scenario 17: click-to-append ---');
check('append mode off (the shipped default)', !doc.getElementById('appendMode').checked);
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Alpha.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('base note delivered', clipboard.includes('Alpha.'), JSON.stringify(clipboard));
check('chip hidden with append mode off', doc.getElementById('appendChip').style.display === 'none');

doc.getElementById('appendToggleBtn').click(); // arm: next dictation appends
check('Append-next button arms the append chip',
  doc.getElementById('appendChip').style.display !== 'none' &&
  doc.getElementById('appendChip').textContent.includes('append'),
  doc.getElementById('appendChip').textContent);
check('box highlighted while armed', doc.getElementById('latest').className.includes('armed'));
check('Append-next button shows the active state', doc.getElementById('appendToggleBtn').className.includes('active'));
doc.getElementById('appendToggleBtn').click(); // second click cancels
check('second click disarms', doc.getElementById('appendChip').style.display === 'none');

// Clicking into the box arms append too (focus handler), so the box lighting up
// and "next dictation appends" are the same gesture.
doc.getElementById('latest').dispatchEvent(new w.Event('focus', { bubbles: false }));
check('focusing the box arms append',
  doc.getElementById('appendChip').style.display !== 'none' &&
  doc.getElementById('latest').className.includes('armed') &&
  doc.getElementById('appendToggleBtn').className.includes('active'),
  doc.getElementById('appendChip').textContent);
doc.getElementById('appendToggleBtn').click(); // disarm to reset state
check('box focus-arm can be cancelled by the button', doc.getElementById('appendChip').style.display === 'none');

doc.getElementById('appendToggleBtn').click(); // re-arm for the real run

fetchQueue.push({ status: 200, body: { text: 'Beta.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('armed dictation appended onto the note', clipboard.includes('Alpha.') && clipboard.includes('Beta.'), JSON.stringify(clipboard));

fetchQueue.push({ status: 200, body: { text: 'Gamma.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('arm is one-shot: the following dictation starts fresh', clipboard.includes('Gamma.') && !clipboard.includes('Beta.'), JSON.stringify(clipboard));

// ===== Scenario 31: editing the dictation boxes (active box + history) =====
console.log('--- scenario 31: editing dictation boxes ---');
// Land a clean note so there is something to edit.
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Patinet stbl.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
const latestBox = doc.getElementById('latest');
check('s31: active box is editable while idle', latestBox.getAttribute('contenteditable') === 'true', latestBox.getAttribute('contenteditable'));

// Hand-edit the active box and fire the input event jsdom needs.
latestBox.textContent = 'Patient stable today.';
latestBox.dispatchEvent(new w.Event('input', { bubbles: true }));
await sleep(20);
clipboard = '';
doc.getElementById('copyBtn').click();
await sleep(40);
check('s31: edited active-box text is what Copy latest copies', clipboard.includes('Patient stable today.'), JSON.stringify(clipboard));
// Copy latest also FILES the note (Option B): the box clears and the slot shows it.
check('s31: Copy latest clears the box', !doc.getElementById('latest').textContent.trim(), JSON.stringify(doc.getElementById('latest').textContent));
check('s31: Copy latest fills the Last dictation slot',
  doc.getElementById('lastDictation').style.display !== 'none' && doc.getElementById('lastDictationText').textContent.includes('Patient stable today.'),
  doc.getElementById('lastDictationText').textContent);

// Bring the filed note back via the slot's "Append to this", then append onto
// it: the dictation must splice onto the EDITED base, not the original typo.
doc.getElementById('lastAppendBtn').click();
check('s31: "Append to this" reloads the filed note into the box', doc.getElementById('latest').textContent.includes('Patient stable today.'), doc.getElementById('latest').textContent);
fetchQueue.push({ status: 200, body: { text: 'Plan unchanged.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s31: append builds on the edited base', clipboard.includes('Patient stable today.') && clipboard.includes('Plan unchanged.') && !clipboard.includes('Patinet stbl.'), JSON.stringify(clipboard));

// The box locks (non-editable) during a session so live/finalize own its text.
fetchQueue.push({ status: 200, body: { text: 'Locked note.' } });
doc.getElementById('recordBtn').click();
await sleep(60);
check('s31: active box is NOT editable mid-session', latestBox.getAttribute('contenteditable') === 'false', latestBox.getAttribute('contenteditable'));
doc.getElementById('recordBtn').click();
await sleep(300);
check('s31: active box is editable again after delivery', latestBox.getAttribute('contenteditable') === 'true', latestBox.getAttribute('contenteditable'));

// History editing: open the list, click into the newest entry, edit it in place.
// Rows are directly contenteditable now (click-to-place-caret); edits persist on
// blur (no Edit/Save buttons).
if (doc.getElementById('history').style.display === 'none') doc.getElementById('toggleHistoryBtn').click();
check('s31: history list is visible', doc.getElementById('history').style.display !== 'none');
const firstItem = doc.querySelector('.history-item');
check('s31: history renders items', !!firstItem);
const itemText = firstItem.querySelector('.history-text');
check('s31: history text is directly editable (contenteditable)', itemText.getAttribute('contenteditable') === 'true', itemText.getAttribute('contenteditable'));
itemText.dispatchEvent(new w.Event('focus', { bubbles: true }));
itemText.textContent = 'Locked note, corrected.';
itemText.dispatchEvent(new w.Event('blur', { bubbles: true })); // persist on blur
await sleep(20);
const savedHist = JSON.parse(w.localStorage.getItem('scribe_v2_transcripts_v9') || '[]');
check('s31: edited history text persisted to storage', savedHist[0] && savedHist[0].text === 'Locked note, corrected.', JSON.stringify(savedHist[0]));
check('s31: edited history entry is stamped editedAt', !!(savedHist[0] && savedHist[0].editedAt));
check('s31: the "edited" marker shows in place', firstItem.querySelector('.history-meta').textContent.includes('edited'), firstItem.querySelector('.history-meta').textContent);

// Pagination: the list shows only the newest 10 by default (so it doesn't take
// over the expanded desktop view), with a "Show N more" control. The top toggle
// still reflects the FULL count.
const totalHist = JSON.parse(w.localStorage.getItem('scribe_v2_transcripts_v9') || '[]').length;
check('s31: enough history to exercise pagination (>10)', totalHist > 10, String(totalHist));
check('s31: only the newest 10 rows render by default', doc.querySelectorAll('.history-item').length === 10, String(doc.querySelectorAll('.history-item').length));
check('s31: the toggle still shows the full count', doc.getElementById('toggleHistoryBtn').textContent.includes('(' + totalHist + ')'), doc.getElementById('toggleHistoryBtn').textContent);
const moreBtn = doc.getElementById('historyMoreBtn');
check('s31: a "Show N more" control is present', !!moreBtn && moreBtn.textContent === 'Show ' + (totalHist - 10) + ' more', moreBtn ? moreBtn.textContent : 'none');
moreBtn.click();
check('s31: "Show more" reveals every row', doc.querySelectorAll('.history-item').length === totalHist, String(doc.querySelectorAll('.history-item').length));
const fewerBtn = doc.getElementById('historyMoreBtn');
check('s31: expanded offers "Show fewer"', !!fewerBtn && fewerBtn.textContent.includes('Show fewer'), fewerBtn ? fewerBtn.textContent : 'none');
fewerBtn.click();
check('s31: "Show fewer" collapses back to 10', doc.querySelectorAll('.history-item').length === 10, String(doc.querySelectorAll('.history-item').length));

// ===== Scenario 32: Last-dictation slot + Copy-files-and-readies (Option B) =====
// The active box holds the current note; finishing it (Copy latest, Clear box,
// or starting a fresh dictation) FILES it into the "Last dictation" slot and
// readies the box for a new note. The slot has its own Copy + "Append to this".
console.log('--- scenario 32: last-dictation slot ---');
doc.getElementById('freshBtn').click(); // clean slate
fetchQueue.push({ status: 200, body: { text: 'Note one.' } });
doc.getElementById('recordBtn').click(); await sleep(120);
doc.getElementById('recordBtn').click(); await sleep(300);
check('s32: note one is in the active box', doc.getElementById('latest').textContent.includes('Note one.'), doc.getElementById('latest').textContent);

// Starting a NEW (fresh) dictation files the previous note into the slot.
fetchQueue.push({ status: 200, body: { text: 'Note two.' } });
doc.getElementById('recordBtn').click(); await sleep(120);
doc.getElementById('recordBtn').click(); await sleep(300);
check('s32: fresh start files the previous note to the slot', doc.getElementById('lastDictationText').textContent.includes('Note one.'), doc.getElementById('lastDictationText').textContent);
check('s32: box now holds the new note only', doc.getElementById('latest').textContent.includes('Note two.') && !doc.getElementById('latest').textContent.includes('Note one.'), doc.getElementById('latest').textContent);

// Slot "Copy" grabs the filed note without disturbing the box.
clipboard = '';
doc.getElementById('lastCopyBtn').click();
await sleep(40);
check('s32: slot Copy copies the filed note', clipboard.includes('Note one.'), JSON.stringify(clipboard));
check('s32: slot Copy leaves the box alone', doc.getElementById('latest').textContent.includes('Note two.'), doc.getElementById('latest').textContent);

// The slot text is hand-editable too: place the caret, edit, and the change
// persists on blur to its history entry (stamped editedAt).
const slotText = doc.getElementById('lastDictationText');
check('s32: slot text is directly editable (contenteditable)', slotText.getAttribute('contenteditable') === 'true', slotText.getAttribute('contenteditable'));
slotText.focus();
slotText.textContent = 'Note one corrected.';
slotText.dispatchEvent(new w.Event('input', { bubbles: true }));
slotText.blur();
await sleep(20);
const histAfterSlotEdit = JSON.parse(w.localStorage.getItem('scribe_v2_transcripts_v9') || '[]');
check('s32: editing the slot persists to its history entry',
  histAfterSlotEdit.some((it) => it.text === 'Note one corrected.' && it.editedAt),
  JSON.stringify(histAfterSlotEdit.map((it) => it.text)));

// The slot now STAYS visible (and editable) mid-session — it no longer
// disappears when a recording starts; the just-filed note shows in it. The
// "Append to this" button is disabled mid-session (it loads into the box).
fetchQueue.push({ status: 200, body: { text: 'Note three.' } });
doc.getElementById('recordBtn').click();
await sleep(60);
check('s32: slot stays visible mid-session', doc.getElementById('lastDictation').style.display !== 'none', doc.getElementById('lastDictation').style.display);
check('s32: "Append to this" is disabled mid-session', doc.getElementById('lastAppendBtn').disabled === true, String(doc.getElementById('lastAppendBtn').disabled));
doc.getElementById('recordBtn').click();
await sleep(300);

// Clearing history empties the slot too (the slot mirrors the saved note).
doc.getElementById('clearBtn').click();
check('s32: clearing history empties the slot', doc.getElementById('lastDictation').style.display === 'none', doc.getElementById('lastDictation').style.display);

// ===== Scenario 18: keyterm presets — injected lists, merge, dedupe, persistence =====
console.log('--- scenario 18: keyterm presets ---');
// Recover the injected preset definitions from the served page so these
// assertions track whatever lists the deployer curates (no hardcoded terms).
const presetSrc = html.match(/const KEYTERM_PRESETS\s*=\s*\((.*)\);/);
check('preset definitions injected into the page', !!presetSrc);
const PRESETS = JSON.parse(presetSrc[1]);
const alwaysTerms = PRESETS.filter((p) => p.always).flatMap((p) => p.terms);
const optional = PRESETS.filter((p) => !p.always);
check('ships an always-on list and at least one optional preset', alwaysTerms.length > 0 && optional.length > 0);
const preset1 = optional[0];
const p1Term = preset1.terms.find((t) => !alwaysTerms.includes(t));
check('optional preset has a distinct term', typeof p1Term === 'string', p1Term);
check('one checkbox per optional preset, none for always-on lists',
  doc.querySelectorAll('#presetRow input[data-preset]').length === optional.length,
  doc.querySelectorAll('#presetRow input[data-preset]').length);

// Leg A (batch, unchecked): always-on terms ride, preset terms do not,
// custom terms lead the merged list (carried on the upload form).
doc.getElementById('freshBtn').click();
doc.getElementById('keyterms').value = 'zebraterm';
fetchQueue.push({ status: 200, body: { text: 'Preset leg A.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
const ktA = JSON.parse(fetchCalls[fetchCalls.length - 1].form.get('keyterms_json'));
check('custom term leads the merged list', ktA[0] === 'zebraterm', JSON.stringify(ktA[0]));
check('always-on terms ride with presets unchecked', alwaysTerms.every((t) => ktA.includes(t)), ktA.length + ' terms');
check('unchecked preset terms are not sent', !ktA.includes(p1Term));
check('leg A delivered normally', clipboard.includes('Preset leg A.'), JSON.stringify(clipboard));

// Check the first optional preset; the choice must persist in settings.
const p1Box = doc.querySelector('input[data-preset="' + preset1.id + '"]');
p1Box.click();
await sleep(400); // debounced settings save
const s18Settings = JSON.parse(w.localStorage.getItem('scribe_v2_settings_v9'));
check('checked preset id persisted in settings (additive v9 field)',
  Array.isArray(s18Settings.presetIds) && s18Settings.presetIds.includes(preset1.id),
  JSON.stringify(s18Settings.presetIds));

// Leg B (batch, checked): preset terms ride; a custom dupe of a preset term
// is sent exactly once.
doc.getElementById('freshBtn').click();
doc.getElementById('keyterms').value = 'zebraterm\n' + p1Term;
fetchQueue.push({ status: 200, body: { text: 'Preset leg B.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
const ktB = JSON.parse(fetchCalls[fetchCalls.length - 1].form.get('keyterms_json'));
check('checked preset terms ride the call',
  preset1.terms.slice(0, 5).every((t) => ktB.includes(t)), ktB.length + ' terms');
check('term duplicated between custom box and preset sent once',
  ktB.filter((t) => t.toLowerCase() === p1Term.toLowerCase()).length === 1);
check('batch keyterm cap respected (<=1000)', ktB.length <= 1000, ktB.length);
check('leg B delivered normally', clipboard.includes('Preset leg B.'), JSON.stringify(clipboard));

// Leg C (batch, checked): the upload form carries the full preset list plus
// the always-on list.
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Preset leg C.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
const ktC = JSON.parse(fetchCalls[fetchCalls.length - 1].form.get('keyterms_json'));
check('batch call carries the full checked preset list', preset1.terms.every((t) => ktC.includes(t)), ktC.length + ' terms');
check('batch call carries the always-on list', alwaysTerms.every((t) => ktC.includes(t)));

// Leg D (batch, unchecked again): preset terms vanish, always-on terms remain
// even with the custom box empty.
p1Box.click();
doc.getElementById('keyterms').value = '';
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Preset leg D.' } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
const ktD = JSON.parse(fetchCalls[fetchCalls.length - 1].form.get('keyterms_json'));
check('unchecking removes preset terms from the next call', !ktD.includes(p1Term));
check('always-on terms survive with box empty and nothing checked', alwaysTerms.every((t) => ktD.includes(t)), ktD.length + ' terms');

// ===== Scenario 19: phone mic session =====
console.log('--- scenario 19: phone mic session ---');
{
  // Fresh boot so session state is clean
  const socks19 = [];
  const fetchCalls19 = [];
  let w19;
  const dom19 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      w19 = win;
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = (url, opts) => { fetchCalls19.push({ url: String(url), opts }); return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ text: 'Phone hello.' }) }); };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks19.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc19 = dom19.window.document;

  // ---- Part A: desktop starts a phone session ----
  const startBtn = doc19.getElementById('phoneStartBtn');
  check('phoneStartBtn exists', !!startBtn);
  if (startBtn) startBtn.click();
  await sleep(20);
  // A WebSocket should have been opened to /api/session/...
  const sessionSock = socks19.find((s) => s.url.includes('/api/session/'));
  check('desktop opens session listener WebSocket', !!sessionSock);

  if (sessionSock) {
    // Simulate phone sending a partial then committed transcript then phone_delivery
    sessionSock.open();
    sessionSock.msg({ message_type: 'partial_transcript', transcript: 'Hello' });
    await sleep(10);
    const latestEl19 = doc19.getElementById('latest');
    check('desktop shows partial from phone', (latestEl19.textContent || '').includes('Hello'));

    sessionSock.msg({ message_type: 'committed_transcript', transcript: 'Hello world.' });
    await sleep(10);
    check('desktop shows committed from phone', (latestEl19.textContent || '').includes('Hello world'));

    sessionSock.msg({ message_type: 'phone_delivery', text: 'Hello world.' });
    await sleep(30);
    check('desktop clipboard gets phone delivery', (w19._clip || '').includes('Hello world'));
  }

  // ---- Part B: phone side — joinedSessionCode appended to WS URL ----
  const socks19b = [];
  const fetchCalls19b = [];
  const dom19b = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = (url, opts) => {
        fetchCalls19b.push({ url: String(url), opts });
        // The /deliver relay answers with a listener ack; the batch upload answers
        // with a transcription.
        if (String(url).includes('/deliver')) {
          return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
        }
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Phone batch."}') });
      };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new win.Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks19b.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc19b = dom19b.window.document;

  // Enter a session code and join
  const joinInput = doc19b.getElementById('phoneJoinInput');
  const joinBtn = doc19b.getElementById('phoneJoinBtn');
  check('phoneJoinInput exists', !!joinInput);
  if (joinInput && joinBtn) {
    joinInput.value = 'ABC123';
    joinBtn.click();
    await sleep(10);

    // The joined phone dictates in BATCH: record -> upload -> deliverFinalText
    // POSTs the authoritative final text to /api/session/ABC123/deliver.
    doc19b.getElementById('apiKey').value = 'test-key';
    doc19b.getElementById('recordBtn').click();
    await sleep(80);
    // No realtime WS opens in batch mode (only the desktop-room sockets, if any).
    check('joined phone opens no realtime transcribe WS', !socks19b.some((s) => s.url.includes('/api/transcribe')));
    doc19b.getElementById('recordBtn').click(); // stop -> upload -> deliver
    await sleep(300);

    // The authoritative final text is POSTed to the room's /deliver endpoint with
    // the joined session code (this is how the desktop clipboard gets written).
    const deliverCall = fetchCalls19b.find((c) => c.url.includes('/api/session/ABC123/deliver') && c.opts && c.opts.method === 'POST' && String(c.opts.body).includes('phone_delivery'));
    check('joined phone POSTs final text to /api/session/ABC123/deliver', !!deliverCall, fetchCalls19b.map((c) => c.url.replace('https://dictation.test', '')).join(','));
    if (deliverCall) {
      let body = {};
      try { body = JSON.parse(deliverCall.opts.body); } catch (e) {}
      check('the /deliver body carries the batch transcript', String(body.text || '').includes('Phone batch.'), JSON.stringify(body.text));
    }
  }
}

// ===== Scenario 20: phone link resilience =====
console.log('--- scenario 20: phone link resilience ---');
{
  // ---- Desktop side: reconnect, replay dedupe, focus-retry, grace fallback ----
  const socks20 = [];
  let w20;
  let clipFail = false; // simulates an unfocused tab: both clipboard paths fail
  const dom20 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      w20 = win;
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { if (clipFail) return Promise.reject(new Error('Document is not focused')); win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks20.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc20 = dom20.window.document;
  const status20 = () => doc20.getElementById('status').textContent;
  const badge20 = () => doc20.getElementById('phoneCodeBadge');

  doc20.getElementById('phoneStartBtn').click();
  await sleep(20);
  const sockA = socks20.find((s) => s.url.includes('/api/session/'));
  check('s20: listener socket opened', !!sockA);
  const code20 = sockA ? sockA.url.split('/api/session/')[1] : '';
  sockA.open();
  await sleep(10);

  // pong frames are heartbeat plumbing, not deliveries
  sockA.msg({ message_type: 'pong' });
  await sleep(10);

  // delivery with an id, then a replay of the same id must be ignored
  sockA.msg({ message_type: 'phone_delivery', text: 'First note.', delivery_id: 'd1' });
  await sleep(30);
  check('s20: delivery copied to the clipboard', (w20._clip || '').includes('First note.'), JSON.stringify(w20._clip));
  check('s20: delivery success status', status20().includes('Done!'), status20());
  w20._clip = 'UNTOUCHED';
  sockA.msg({ message_type: 'phone_delivery', text: 'First note.', delivery_id: 'd1' });
  await sleep(30);
  check('s20: replayed delivery_id deduped', w20._clip === 'UNTOUCHED', JSON.stringify(w20._clip));

  // phone_session_end no longer tears the session down (multi-dictation sessions)
  sockA.msg({ message_type: 'phone_session_end' });
  await sleep(30);
  check('s20: session_end keeps the listener socket open', !sockA.closed);
  check('s20: badge still shows the code after session_end', badge20().style.display !== 'none' && badge20().textContent.includes(code20), badge20().textContent);

  // dropped socket -> loud status, warn badge, auto-reconnect to the same room
  sockA.serverClose();
  await sleep(30);
  check('s20: drop is loud', status20().includes('reconnecting'), status20());
  check('s20: badge flags the dead link', badge20().textContent.includes('⚠'), badge20().textContent);
  // Returning the desktop to the foreground reconnects the listener IMMEDIATELY
  // (the room then replays the buffered delivery) instead of waiting out the
  // throttled backoff timer — the fix for "deliveries don't land after the
  // desktop was backgrounded". No 1s wait needed.
  w20.dispatchEvent(new w20.Event('focus'));
  await sleep(40);
  const sessSocks20 = socks20.filter((s) => s.url.includes('/api/session/'));
  const sockB = sessSocks20[1];
  check('s20: foreground reconnects the listener at once (no backoff wait)', !!sockB && sockB.url.includes(code20), sessSocks20.length + ' session sockets');
  sockB.open();
  await sleep(10);
  check('s20: badge recovers once reconnected', !badge20().textContent.includes('⚠'), badge20().textContent);

  // the room replays the held delivery on reconnect; a new id must deliver
  sockB.msg({ message_type: 'phone_delivery', text: 'Held note.', delivery_id: 'd2' });
  await sleep(30);
  check('s20: held delivery lands after reconnect', (w20._clip || '').includes('Held note.'), JSON.stringify(w20._clip));

  // focus-retry: copy fails while "unfocused", retries on the focus event
  clipFail = true;
  sockB.msg({ message_type: 'phone_delivery', text: 'Blocked note.', delivery_id: 'd3' });
  await sleep(30);
  check('s20: failed copy is loud and warns against pasting', status20().includes('FAILED'), status20());
  check('s20: clipboard untouched by the failed copy', !(w20._clip || '').includes('Blocked note.'), JSON.stringify(w20._clip));
  clipFail = false;
  w20.dispatchEvent(new w20.Event('focus'));
  await sleep(30);
  check('s20: refocus retries the copy', (w20._clip || '').includes('Blocked note.'), JSON.stringify(w20._clip));
  check('s20: refocus success status', status20().includes('Done!'), status20());

  // session_end grace window: live committed text is delivered only after the
  // authoritative delivery had its chance
  sockB.msg({ message_type: 'committed_transcript', transcript: 'Live only words.' });
  sockB.msg({ message_type: 'phone_session_end' });
  await sleep(30);
  check('s20: fallback waits for the real delivery first', !(w20._clip || '').includes('Live only words.'), JSON.stringify(w20._clip));
  check('s20: waiting status during the grace window', status20().includes('waiting'), status20());
  await sleep(10500); // > PHONE_FALLBACK_GRACE_MS
  check('s20: grace fallback delivers the live text', (w20._clip || '').includes('Live only words.'), JSON.stringify(w20._clip));
  check('s20: fallback framed as degraded', status20().includes('Verify'), status20());

  // a real delivery cancels a pending fallback (no stale overwrite later)
  sockB.msg({ message_type: 'committed_transcript', transcript: 'Stale live.' });
  sockB.msg({ message_type: 'phone_session_end' });
  await sleep(30);
  sockB.msg({ message_type: 'phone_delivery', text: 'Authoritative note.', delivery_id: 'd4' });
  await sleep(10500);
  check('s20: delivery cancels the grace fallback', (w20._clip || '').includes('Authoritative note.') && !(w20._clip || '').includes('Stale live.'), JSON.stringify(w20._clip));

  // Desktop OWNS append in the phone-link flow: with THIS device's append mode
  // on, an incoming phone delivery EXTENDS the current note instead of replacing
  // it (mirrors single-desktop append) — this is the fix for "append works on
  // the desktop but the phone mic doesn't respect it". The dedupe ring still
  // guards replays so a retried delivery can never double-append onto a chart.
  doc20.getElementById('appendMode').click();
  check('s20: append mode on for the desktop-append check', doc20.getElementById('appendMode').checked);
  w20._clip = '';
  sockB.msg({ message_type: 'phone_delivery', text: 'Plus one.', delivery_id: 'd5' });
  await sleep(40);
  check('s20: phone delivery appends onto the desktop note (append mode on)', (w20._clip || '').includes('Authoritative note.') && (w20._clip || '').includes('Plus one.'), JSON.stringify(w20._clip));
  check('s20: the append outcome is announced', status20().includes('appended'), status20());
  // A replay of an already-seen id must NOT append again (no double-paste).
  w20._clip = 'UNTOUCHED2';
  sockB.msg({ message_type: 'phone_delivery', text: 'Plus one.', delivery_id: 'd5' });
  await sleep(40);
  check('s20: replayed append delivery deduped (no double-append)', w20._clip === 'UNTOUCHED2', JSON.stringify(w20._clip));
  // Append mode back off => the next delivery replaces again (single segment).
  doc20.getElementById('appendMode').click();
  w20._clip = '';
  sockB.msg({ message_type: 'phone_delivery', text: 'Fresh replace.', delivery_id: 'd6' });
  await sleep(40);
  check('s20: append mode off => delivery replaces', (w20._clip || '').includes('Fresh replace.') && !(w20._clip || '').includes('Plus one.'), JSON.stringify(w20._clip));

  // End session: everything closes and stays closed (no reconnect loop)
  doc20.getElementById('phoneStopBtn').click();
  await sleep(1500);
  const sessSocksEnd = socks20.filter((s) => s.url.includes('/api/session/'));
  check('s20: stop closes the listener for good', sessSocksEnd.every((s) => s.closed) && sessSocksEnd.length === 2, sessSocksEnd.length + ' session sockets');

  // ---- Phone side: a deliver ack with zero listeners must be loud ----
  const socks20p = [];
  const fetch20p = [];
  const dom20p = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = (url, opts) => {
        fetch20p.push({ url: String(url), opts });
        if (String(url).includes('/deliver')) {
          return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":0}') });
        }
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Phone note."}') });
      };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new win.Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks20p.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc20p = dom20p.window.document;
  const status20p = () => doc20p.getElementById('status').textContent;

  // The joined phone dictates in BATCH and relays the authoritative final text.
  doc20p.getElementById('phoneJoinInput').value = 'ABC123';
  doc20p.getElementById('phoneJoinBtn').click();
  doc20p.getElementById('apiKey').value = 'test-key';
  doc20p.getElementById('recordBtn').click();
  await sleep(80);
  check('s20p: joined phone opens no realtime transcribe WS', !socks20p.some((s) => s.url.includes('/api/transcribe')));
  doc20p.getElementById('recordBtn').click(); // stop -> upload -> deliver
  await sleep(600); // upload -> finalize -> deliver -> relay ack
  const dCall = fetch20p.find((c) => c.url.includes('/deliver') && c.opts && String(c.opts.body).includes('phone_delivery'));
  check('s20p: deliver POST sent', !!dCall);
  const dBody = dCall ? JSON.parse(dCall.opts.body) : {};
  check('s20p: deliver carries text + delivery_id', dBody.text && dBody.text.includes('Phone note.') && typeof dBody.delivery_id === 'string' && dBody.delivery_id.length > 0, JSON.stringify(dBody.delivery_id));
  check('s20p: zero-listener ack is loud on the phone', status20p().includes('Desktop link is DOWN'), status20p());
}

// ===== Scenario 20q: phone-side durable delivery queue (durability) =====
// A joined phone whose /deliver POST fails (or acks zero listeners) must not
// drop the text: it is persisted and retried on link heal / at boot until a
// desktop listener acks it. The desktop dedupes retries by a ring of ids so a
// re-POST can never re-copy stale text onto a chart.
console.log('--- scenario 20q: phone delivery queue ---');
{
  // ---- Phone side: a failed POST enqueues; an online heal re-delivers ----
  const socks20q = [];
  const fetch20q = [];
  let deliverMode = 'fail'; // 'fail' | 'zero' | 'ok'
  const dom20q = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = (url, opts) => {
        fetch20q.push({ url: String(url), opts });
        if (String(url).includes('/deliver')) {
          if (deliverMode === 'fail') return Promise.resolve({ ok: false, status: 500, text: () => Promise.resolve('boom') });
          const listeners = deliverMode === 'ok' ? 1 : 0;
          return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":' + listeners + '}') });
        }
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Queue note."}') });
      };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new win.Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks20q.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc20q = dom20q.window.document;
  const win20q = dom20q.window;
  const status20q = () => doc20q.getElementById('status').textContent;
  const queue20q = () => { try { return JSON.parse(win20q.localStorage.getItem('scribe_v2_settings_v9')).pendingDeliveries || []; } catch (e) { return []; } };

  doc20q.getElementById('phoneJoinInput').value = 'QUE111';
  doc20q.getElementById('phoneJoinBtn').click();
  doc20q.getElementById('apiKey').value = 'test-key';
  doc20q.getElementById('recordBtn').click();
  await sleep(80);
  doc20q.getElementById('recordBtn').click(); // stop -> upload -> deliver (fails)
  await sleep(600);

  // Only the transcript deliveries — not the phone_join ping that joining fires.
  const deliveries20q = () => fetch20q.filter((c) => c.url.includes('/deliver') && c.opts && String(c.opts.body).includes('phone_delivery'));
  const firstDeliver = deliveries20q()[0];
  check('s20q: a /deliver POST was attempted', !!firstDeliver);
  const firstId = firstDeliver ? JSON.parse(firstDeliver.opts.body).delivery_id : '';
  check('s20q: a failed relay is loud', status20q().includes('FAILED') && status20q().includes('NOT'), status20q());
  check('s20q: the undelivered text is queued (durable in localStorage)', queue20q().length === 1 && queue20q()[0].text.includes('Queue note.'), JSON.stringify(queue20q()));

  // Heal the link: an online event drains the queue, re-POSTing the held item.
  const deliverCount0 = deliveries20q().length;
  deliverMode = 'ok';
  win20q.dispatchEvent(new win20q.Event('online'));
  await sleep(200);
  const retries = deliveries20q().slice(deliverCount0);
  check('s20q: the heal triggers a retry POST', retries.length >= 1, retries.length + ' retries');
  check('s20q: the retry reuses the SAME delivery_id (so the desktop ring dedupes a double)', retries.length > 0 && JSON.parse(retries[0].opts.body).delivery_id === firstId, firstId);
  check('s20q: a delivered item clears the queue', queue20q().length === 0, JSON.stringify(queue20q()));
  check('s20q: a drained queue gives quiet positive closure', status20q().includes('delivered to the desktop'), status20q());

  // Leave KEEPS the queued deliveries (code-bound) — the old wipe here
  // destroyed the undelivered note a clinician was trying to recover, because
  // re-linking passes through Leave. The user's field incident.
  deliverMode = 'fail';
  doc20q.getElementById('recordBtn').click();
  await sleep(80);
  doc20q.getElementById('recordBtn').click();
  await sleep(600);
  check('s20q: a second failure re-queues', queue20q().length === 1, JSON.stringify(queue20q()));
  check('s20q: the queued item is code-bound', queue20q()[0] && queue20q()[0].code === 'QUE111', JSON.stringify(queue20q()[0]));
  doc20q.getElementById('phoneLeaveBtn').click();
  await sleep(30);
  check('s20q: Leave KEEPS the code-bound queue (text no longer destroyed)', queue20q().length === 1, JSON.stringify(queue20q()));
  check('s20q: Leave says the undelivered note is kept', status20q().includes('undelivered note') && status20q().includes('kept'), status20q());
  check('s20q: the queue chip shows the stranded note while unjoined', doc20q.getElementById('queueChip').style.display !== 'none' && doc20q.getElementById('queueChip').textContent.includes('another desktop'), doc20q.getElementById('queueChip').textContent);

  // Joining a DIFFERENT desktop: the item is kept but NEVER auto-POSTed to the
  // wrong code (stale text must not land on the wrong chart).
  deliverMode = 'ok';
  const deliverCount1 = deliveries20q().length;
  doc20q.getElementById('phoneJoinInput').value = 'OTHER9';
  doc20q.getElementById('phoneJoinBtn').click();
  await sleep(300);
  const wrongCodePosts = deliveries20q().slice(deliverCount1).filter((c) => c.url.includes('/api/session/OTHER9/'));
  check('s20q: a different-code join never POSTs the old-code item', wrongCodePosts.length === 0, wrongCodePosts.map((c) => c.url).join(','));
  check('s20q: the item survives the different-code join', queue20q().length === 1 && queue20q()[0].code === 'QUE111', JSON.stringify(queue20q()));

  // REJOINING the original code auto-flushes it — the incident self-heals.
  doc20q.getElementById('phoneLeaveBtn').click();
  await sleep(30);
  doc20q.getElementById('phoneJoinInput').value = 'QUE111';
  doc20q.getElementById('phoneJoinBtn').click();
  await sleep(300);
  const rejoinPosts = deliveries20q().filter((c) => c.url.includes('/api/session/QUE111/'));
  check('s20q: rejoining the SAME code auto-delivers the stranded note', queue20q().length === 0 && rejoinPosts.length > deliverCount1 - 1, JSON.stringify(queue20q()));
  check('s20q: the queue chip clears once delivered', doc20q.getElementById('queueChip').style.display === 'none', doc20q.getElementById('queueChip').style.display);
}

{
  // ---- Boot recovery: a persisted queue flushes at startup (phone crash) ----
  const fetch20q2 = [];
  const dom20q2 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = (url, opts) => {
        fetch20q2.push({ url: String(url), opts });
        if (String(url).includes('/deliver')) return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"x"}') });
      };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() {} stop() {} };
      const SockClass = class extends MockWS { constructor(url) { super(url); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
      // Seed a persisted join + an undelivered queued delivery: a phone that
      // died after transcribing but before its relay landed. The item has NO
      // code field (a legacy pre-code-binding queue) — loadSettings must stamp
      // it with the persisted join so it still flushes after the update.
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({
        saveApiKey: true,
        joinedSessionCode: 'BOOT22',
        pendingDeliveries: [{ id: 'boot-1', text: 'Recovered note.', ts: Date.now() }],
      }));
    },
  });
  await sleep(150);
  const win20q2 = dom20q2.window;
  const bootDeliver = fetch20q2.find((c) => c.url.includes('/api/session/BOOT22/deliver') && c.opts && c.opts.method === 'POST');
  check('s20q2: a persisted queue re-POSTs its delivery at boot (crash recovery)', !!bootDeliver, fetch20q2.map((c) => c.url.replace('https://dictation.test', '')).join(','));
  check('s20q2: the boot retry carries the persisted id + text', !!bootDeliver && JSON.parse(bootDeliver.opts.body).delivery_id === 'boot-1' && JSON.parse(bootDeliver.opts.body).text.includes('Recovered note.'), bootDeliver && bootDeliver.opts.body);
  const queue20q2 = () => { try { return JSON.parse(win20q2.localStorage.getItem('scribe_v2_settings_v9')).pendingDeliveries || []; } catch (e) { return []; } };
  check('s20q2: the recovered delivery clears the queue once acked', queue20q2().length === 0, JSON.stringify(queue20q2()));
}

{
  // ---- Desktop ring-dedupe: a retried/out-of-order re-POST can't re-copy ----
  const socks20q3 = [];
  let w20q3;
  const dom20q3 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      w20q3 = win;
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() {} stop() {} };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks20q3.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc20q3 = dom20q3.window.document;
  doc20q3.getElementById('phoneStartBtn').click();
  await sleep(20);
  const sock = socks20q3.find((s) => s.url.includes('/api/session/'));
  check('s20q3: desktop listener socket opened', !!sock);
  sock.open();
  await sleep(10);

  sock.msg({ message_type: 'phone_delivery', text: 'Note one.', delivery_id: 'q1' });
  await sleep(20);
  check('s20q3: first delivery copied', (w20q3._clip || '').includes('Note one.'), JSON.stringify(w20q3._clip));
  sock.msg({ message_type: 'phone_delivery', text: 'Note two.', delivery_id: 'q2' });
  await sleep(20);
  check('s20q3: newer delivery copied', (w20q3._clip || '').includes('Note two.'), JSON.stringify(w20q3._clip));
  // A late retry of q1 arrives AFTER q2 — with the OLD single-id dedupe it would
  // overwrite the clipboard with stale text. The ring must drop it.
  w20q3._clip = 'KEEP';
  sock.msg({ message_type: 'phone_delivery', text: 'Note one.', delivery_id: 'q1' });
  await sleep(20);
  check('s20q3: a stale re-POST after a newer one is deduped by the ring', w20q3._clip === 'KEEP', JSON.stringify(w20q3._clip));
  // A genuinely new id still lands.
  sock.msg({ message_type: 'phone_delivery', text: 'Note three.', delivery_id: 'q3' });
  await sleep(20);
  check('s20q3: a fresh id still lands after the dedupes', (w20q3._clip || '').includes('Note three.'), JSON.stringify(w20q3._clip));
}

// ===== Scenario 21: SessionRoom Durable Object contract (direct, no jsdom) =====
console.log('--- scenario 21: session room DO contract ---');
{
  class FakeSock {
    constructor() { this.sent = []; this.handlers = {}; }
    accept() {}
    send(d) { this.sent.push(d); }
    addEventListener(ev, fn) { this.handlers[ev] = fn; }
  }
  let lastPair = null;
  globalThis.WebSocketPair = function () {
    lastPair = { client: new FakeSock(), server: new FakeSock() };
    return { 0: lastPair.client, 1: lastPair.server };
  };
  const RealResponse = globalThis.Response;
  globalThis.Response = class {
    constructor(body, init) { this.body = body; const i = init || {}; this.status = i.status || 200; this.webSocket = i.webSocket; }
    async text() { return this.body; }
  };

  const room = new worker.SessionRoom({}, {});
  const wsReq = () => ({ headers: { get: (h) => (h === 'Upgrade' ? 'websocket' : null) }, method: 'GET', url: 'https://room/api/session/ABC123' });
  const postReq = (body) => ({ headers: { get: () => null }, method: 'POST', url: 'https://room/broadcast', text: async () => body });

  await room.fetch(wsReq());
  const listenerA = lastPair.server;
  listenerA.handlers.message({ data: JSON.stringify({ message_type: 'ping' }) });
  check('s21: room answers ping with pong', listenerA.sent.some((d) => JSON.parse(d).message_type === 'pong'));

  const delivery = JSON.stringify({ message_type: 'phone_delivery', text: 'DO note.', delivery_id: 'do1' });
  const ack1 = JSON.parse(await (await room.fetch(postReq(delivery))).text());
  check('s21: broadcast acks the listener count', ack1.listeners === 1, JSON.stringify(ack1));
  check('s21: listener received the delivery', listenerA.sent.includes(delivery));

  listenerA.handlers.close();
  const ack0 = JSON.parse(await (await room.fetch(postReq(delivery))).text());
  check('s21: zero listeners reported after the socket closes', ack0.listeners === 0, JSON.stringify(ack0));

  await room.fetch(wsReq());
  const listenerB = lastPair.server;
  check('s21: reconnecting listener gets the held delivery replayed', listenerB.sent.includes(delivery), listenerB.sent.length + ' frames');

  room.lastDelivery.ts -= 3 * 60 * 1000; // age it past the replay window
  await room.fetch(wsReq());
  const listenerC = lastPair.server;
  check('s21: stale deliveries are not replayed', !listenerC.sent.includes(delivery), listenerC.sent.length + ' frames');

  const ackNonDelivery = JSON.parse(await (await room.fetch(postReq(JSON.stringify({ message_type: 'partial_transcript', text: 'x' })))).text());
  check('s21: transcript frames are not buffered as deliveries', room.lastDelivery.body === delivery && typeof ackNonDelivery.listeners === 'number');

  // GET /latest: native pollers (AHK) read the held delivery without joining
  const latestReq = () => ({ headers: { get: () => null }, method: 'GET', url: 'https://room/api/session/ABC123/latest' });
  const staleLatest = JSON.parse(await (await room.fetch(latestReq())).text());
  check('s21: /latest returns null for a stale delivery', staleLatest.ok === true && staleLatest.delivery === null, JSON.stringify(staleLatest));
  const delivery2 = JSON.stringify({ message_type: 'phone_delivery', text: 'DO note 2.', delivery_id: 'do2' });
  await room.fetch(postReq(delivery2));
  const freshLatest = JSON.parse(await (await room.fetch(latestReq())).text());
  check('s21: /latest returns the held delivery with its id', freshLatest.delivery && freshLatest.delivery.delivery_id === 'do2' && freshLatest.delivery.text === 'DO note 2.' && typeof freshLatest.age_ms === 'number', JSON.stringify(freshLatest));

  globalThis.Response = RealResponse;
  delete globalThis.WebSocketPair;
}

// ===== Scenario 22: phone link persistence — resume + rejoin across reloads =====
console.log('--- scenario 22: phone link persistence ---');
{
  // ---- Desktop: a persisted session resumes at boot; persisted delivery id dedupes the replay ----
  const socks22 = [];
  let w22;
  const dom22 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      w22 = win;
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks22.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
      // A desktop session was active before this "reload"
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ phoneSessionCode: 'ROOMZZ', lastDeliveryId: 'old1' }));
    },
  });
  await sleep(100);
  const doc22 = dom22.window.document;
  const settings22 = () => JSON.parse(w22.localStorage.getItem('scribe_v2_settings_v9'));

  const resumeSock = socks22.find((s) => s.url.includes('/api/session/ROOMZZ'));
  check('s22: persisted session reconnects at boot', !!resumeSock);
  check('s22: session UI restored (badge + stop button)', doc22.getElementById('phoneCodeBadge').style.display !== 'none' && doc22.getElementById('phoneStopBtn').style.display !== 'none');
  resumeSock.open();
  await sleep(10);

  w22._clip = 'PRISTINE';
  resumeSock.msg({ message_type: 'phone_delivery', text: 'Old note.', delivery_id: 'old1' }); // room replay of a pre-reload delivery
  await sleep(30);
  check('s22: persisted delivery id dedupes the replay across the reload', w22._clip === 'PRISTINE', JSON.stringify(w22._clip));
  resumeSock.msg({ message_type: 'phone_delivery', text: 'Post-reload note.', delivery_id: 'new1' });
  await sleep(30);
  check('s22: new delivery lands after the resume', (w22._clip || '').includes('Post-reload note.'), JSON.stringify(w22._clip));
  check('s22: new delivery id persisted immediately', settings22().lastDeliveryId === 'new1', JSON.stringify(settings22().lastDeliveryId));

  doc22.getElementById('phoneStopBtn').click();
  await sleep(20);
  check('s22: ending the session forgets the persisted code', settings22().phoneSessionCode === '', JSON.stringify(settings22().phoneSessionCode));
  doc22.getElementById('phoneStartBtn').click();
  await sleep(20);
  check('s22: starting a session persists its code', /^[A-Z2-9]{6}$/.test(settings22().phoneSessionCode), JSON.stringify(settings22().phoneSessionCode));
  doc22.getElementById('phoneStopBtn').click();

  // ---- Phone: a persisted join rides the next dictation after a "reload"; Leave forgets it ----
  const socks22p = [];
  const fetch22p = [];
  let w22p;
  const dom22p = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      w22p = win;
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = (url, opts) => {
        fetch22p.push({ url: String(url), opts: opts || {} });
        if (String(url).includes('/deliver')) {
          return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
        }
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Rejoin batch."}') });
      };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new win.Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks22p.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
      // The phone joined a desktop session before this "reload" (iOS PWA kill)
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ joinedSessionCode: 'ABC123', engine: 'batch' }));
    },
  });
  await sleep(100);
  const doc22p = dom22p.window.document;
  const settings22p = () => JSON.parse(w22p.localStorage.getItem('scribe_v2_settings_v9'));

  check('s22p: join badge restored at boot', doc22p.getElementById('phoneJoinBadge').style.display !== 'none');
  check('s22p: leave button shown for the restored join', doc22p.getElementById('phoneLeaveBtn').style.display !== 'none');
  doc22p.getElementById('apiKey').value = 'test-key';
  // The restored join rides the next BATCH dictation: the authoritative final
  // text is relayed to the desktop via the /deliver POST carrying the code.
  doc22p.getElementById('recordBtn').click();
  await sleep(80);
  check('s22p: joined phone opens no realtime transcribe WS', !socks22p.some((s) => s.url.includes('/api/transcribe')));
  doc22p.getElementById('recordBtn').click(); // stop -> upload -> deliver
  await sleep(400);
  const rejoinDeliver = fetch22p.find((c) => c.url.includes('/api/session/ABC123/deliver') && c.opts && c.opts.method === 'POST' && String(c.opts.body).includes('phone_delivery'));
  check('s22p: restored join rides the next dictation (relays to /deliver with the code)', !!rejoinDeliver, fetch22p.map((c) => c.url.replace('https://dictation.test', '')).join(','));

  doc22p.getElementById('phoneLeaveBtn').click();
  await sleep(20);
  check('s22p: leave forgets the persisted join', settings22p().joinedSessionCode === '', JSON.stringify(settings22p().joinedSessionCode));
  check('s22p: leave hides the badge', doc22p.getElementById('phoneJoinBadge').style.display === 'none');
}

// ===== Scenario 23: iOS mic resilience — wake lock, muted-track rebuild, re-warm fallback =====
console.log('--- scenario 23: iOS mic resilience ---');

// Leg A (main DOM): a dictation holds a screen wake lock until delivery
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Wake lock note.' } });
const wlBefore = wakeLockCalls;
doc.getElementById('recordBtn').click();
await sleep(120);
check('s23: wake lock acquired for the dictation', wakeLockCalls === wlBefore + 1 && activeWakeLock !== null, wakeLockCalls - wlBefore);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s23: dictation delivered', clipboard.includes('Wake lock note.'), JSON.stringify(clipboard));
check('s23: wake lock released after delivery', activeWakeLock === null);

// Leg B (main DOM): an iOS-interrupted track ("live" but muted) forces a mic rebuild
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Rebuilt mic note.' } });
micTrack.muted = true;
const gumBefore = gumCalls;
doc.getElementById('recordBtn').click();
micTrack.readyState = 'live'; micTrack.muted = false; // the fresh acquisition hands back a working track
await sleep(120);
check('s23: muted track triggers a mic re-acquisition', gumCalls === gumBefore + 1, gumCalls - gumBefore);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s23: dictation works on the rebuilt mic', clipboard.includes('Rebuilt mic note.'), JSON.stringify(clipboard));

// Leg C (fresh DOM, no Permissions API — the iOS Safari situation): the
// re-warm on visibilitychange must still re-engage a dead mic.
{
  const track23 = { readyState: 'live', muted: false, addEventListener() {}, stop() { this.readyState = 'ended'; } };
  const stream23 = { getAudioTracks: () => [track23], getTracks: () => [track23] };
  let gum23 = 0;
  let gumFail23 = 0; // make the next N acquisitions fail (iOS hands the audio session back late)
  let w23;
  const dom23 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      w23 = win;
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      // NOTE: no win.navigator.permissions — like iOS Safari for the mic
      // jsdom reports visibilityState 'prerender'; the re-warm only runs when visible
      Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
      win.navigator.mediaDevices = { getUserMedia: () => { gum23++; if (gumFail23 > 0) { gumFail23--; return Promise.reject(new Error('NotReadableError')); } track23.readyState = 'live'; track23.muted = false; return Promise.resolve(stream23); }, addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Warm note."}') });
      win.MediaRecorder = class {
        constructor(s) { this.state = 'inactive'; }
        static isTypeSupported() { return false; }
        start() { this.state = 'recording'; }
        stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); }
      };
      win.WebSocket = MockWS;
    },
  });
  await sleep(100);
  const doc23 = dom23.window.document;
  check('s23c: no warm-up without a prior grant (no prompt ambush)', gum23 === 0, gum23);
  doc23.getElementById('apiKey').value = 'test-key';
  doc23.getElementById('sonioxKey').value = 'test-skey';
  doc23.getElementById('recordBtn').click();
  await sleep(120);
  check('s23c: first dictation acquires the mic', gum23 === 1, gum23);
  doc23.getElementById('recordBtn').click();
  await sleep(300);
  check('s23c: batch note delivered', (w23._clip || '').includes('Warm note.'), JSON.stringify(w23._clip));
  track23.readyState = 'ended'; // iOS killed the stream while the page was hidden
  doc23.dispatchEvent(new dom23.window.Event('visibilitychange'));
  await sleep(80);
  check('s23c: visibility re-warm re-engages the mic without the Permissions API', gum23 === 2, gum23);

  // iOS hands the audio session back late: the first re-acquire fails, the
  // backoff retry must recover without any user action.
  gumFail23 = 1;
  track23.readyState = 'ended';
  doc23.dispatchEvent(new dom23.window.Event('visibilitychange'));
  await sleep(80);
  check('s23c: flaky first re-acquire attempted', gum23 === 3, gum23);
  await sleep(900); // > 700ms retry backoff
  check('s23c: re-warm retries and recovers on its own', gum23 === 4, gum23);

  // Standalone PWAs can fire only focus (no visibilitychange) on app switch
  track23.readyState = 'ended';
  dom23.window.dispatchEvent(new dom23.window.Event('focus'));
  await sleep(80);
  check('s23c: window focus re-engages a dead mic', gum23 === 5, gum23);
}

// Leg D (fresh DOM): the grant persists, so a killed-and-relaunched iOS PWA
// re-warms the mic at boot instead of staying cold until the first press.
{
  const track23d = { readyState: 'live', muted: false, addEventListener() {}, stop() { this.readyState = 'ended'; } };
  const stream23d = { getAudioTracks: () => [track23d], getTracks: () => [track23d] };
  let gum23d = 0;
  const dom23d = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => Promise.resolve() };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      // no Permissions API (iOS Safari); the persisted grant is the only signal
      Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
      win.navigator.mediaDevices = { getUserMedia: () => { gum23d++; return Promise.resolve(stream23d); }, addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.onstop) this.onstop(); } };
      win.WebSocket = MockWS;
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ micGranted: true }));
    },
  });
  await sleep(150);
  check('s23d: persisted grant re-warms the mic at boot after a PWA relaunch', gum23d === 1, gum23d);
}

// ===== Scenario 23r: resume silent-capture bug =====
// The critical real-world failure: joined PWA, resume the session, press the big
// button, the mic LOOKS engaged but records nothing. Root cause: iOS leaves a
// track that died while backgrounded reporting readyState "live"/unmuted, so
// audioGraphHealthy() trusts a corpse and ensureAudio() reuses a dead graph.
// Fix: any backgrounding marks the graph suspect -> ensureAudio rebuilds.
console.log('--- scenario 23r: resume forces a mic rebuild + interruption alarm ---');

// Baseline: with NO backgrounding, a healthy graph is reused (no re-acquire).
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Reuse note.' } });
micTrack.readyState = 'live'; micTrack.muted = false;
const gumReuseBefore = gumCalls;
doc.getElementById('recordBtn').click();
await sleep(60);
check('s23r: a healthy graph (no backgrounding) is reused — no re-acquire', gumCalls === gumReuseBefore, gumCalls - gumReuseBefore);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s23r: the reused-graph dictation delivers', clipboard.includes('Reuse note.'), JSON.stringify(clipboard));

// Background the PWA (pagehide on iOS): the track still reports "live"/unmuted
// (the iOS corpse), but the next press MUST rebuild rather than record silence.
w.dispatchEvent(new w.Event('pagehide'));
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Resumed note.' } });
micTrack.readyState = 'live'; micTrack.muted = false; // looks alive — the bug condition
const gumResumeBefore = gumCalls;
doc.getElementById('recordBtn').click();
await sleep(60);
check('s23r: a backgrounded graph is REBUILT despite looking healthy (the resume bug fix)', gumCalls === gumResumeBefore + 1, gumCalls - gumResumeBefore);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s23r: dictation works after the forced rebuild', clipboard.includes('Resumed note.'), JSON.stringify(clipboard));

// visibilitychange -> hidden also marks the graph suspect.
Object.defineProperty(doc, 'visibilityState', { value: 'hidden', configurable: true });
doc.dispatchEvent(new w.Event('visibilitychange'));
Object.defineProperty(doc, 'visibilityState', { value: 'visible', configurable: true });
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Hidden note.' } });
const gumHiddenBefore = gumCalls;
doc.getElementById('recordBtn').click();
await sleep(60);
check('s23r: visibilitychange->hidden also forces a rebuild', gumCalls === gumHiddenBefore + 1, gumCalls - gumHiddenBefore);
doc.getElementById('recordBtn').click();
await sleep(300);

// A mid-dictation AudioContext interruption (Siri/call/another app) must fire
// the alarm even though the frozen analyser keeps reading "speech" (micRms high,
// so the RMS-flatline check can never fire — only the state check catches it).
// BUT it must DEBOUNCE: iOS emits spurious interrupted->running blips that drop
// no audio, and the old instant-fire turned those into failed dictations (the
// mic-stability regression). Only a SUSTAINED interruption (> the grace) alarms.
doc.getElementById('freshBtn').click();
fetchQueue.push({ status: 200, body: { text: 'Interrupted note.' } });
micRms = 0.05; // real "speech" level so only the AudioContext-state check can alarm
doc.getElementById('recordBtn').click();
micTrack.readyState = 'live'; micTrack.muted = false; // a healthy live track (rebuild hands one back)
await sleep(250); // past the 200ms start-up grace; no alarm yet (track live, rms high)
const ctxR = mainCtxInstances[mainCtxInstances.length - 1];
// A transient blip shorter than the grace must NOT fail a healthy take.
ctxR.state = 'interrupted';
await sleep(150); // < CTX_INTERRUPT_GRACE_MS (400ms)
ctxR.state = 'running'; // snaps back before the grace elapses
await sleep(120);
check('s23r: a transient AudioContext blip does NOT alarm (regression fix)', !/AUDIO INTERRUPTED/i.test(status()), status());
// A sustained interruption (audio actually lost, analyser frozen) still alarms.
ctxR.state = 'interrupted'; // the audio session is taken over mid-dictation
await sleep(500); // > the grace; several 30ms watchdog ticks
check('s23r: a sustained AudioContext interruption fires the mic alarm', /AUDIO INTERRUPTED/i.test(status()) && statusCls().includes('err'), status());
ctxR.state = 'running'; // release; finish the session so later state is clean
doc.getElementById('recordBtn').click();
await sleep(300);

// ===== Scenario 24: QR join =====
console.log('--- scenario 24: QR join ---');
{
  // ---- Desktop: the rendered QR must decode (with a real decoder) to the join URL ----
  const socks23 = [];
  const dom23 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => Promise.resolve() };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => { micTrack.readyState = 'live'; micTrack.muted = false; return Promise.resolve(mockStream); }, addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks23.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(100);
  const doc23 = dom23.window.document;

  doc23.getElementById('phoneStartBtn').click();
  await sleep(20);
  const qrEl = doc23.getElementById('phoneQr');
  const code23 = doc23.getElementById('phoneCodeBadge').textContent.trim();
  check('s24: QR rendered when the session starts', qrEl.style.display !== 'none' && qrEl.innerHTML.includes('<svg'));
  const joinUrl = qrEl.getAttribute('data-join-url');
  check('s24: QR advertises the join URL for this session', joinUrl === 'https://dictation.test/?join=' + code23, joinUrl);

  // Rasterize the SVG modules and decode with jsqr — proves the hand-rolled
  // encoder produces a genuinely scannable code, not just plausible pixels.
  const svg = qrEl.innerHTML;
  const dim = Number((svg.match(/viewBox="0 0 (\d+)/) || [])[1] || 0);
  check('s24: QR has a sane module count', dim >= 29 && dim <= 49, dim); // v1..v6 + quiet zones
  const grid = Array.from({ length: dim }, () => new Array(dim).fill(0));
  for (const mod of svg.matchAll(/M(\d+) (\d+)h1v1h-1z/g)) grid[Number(mod[2])][Number(mod[1])] = 1;
  const scale = 4, W = dim * scale;
  const px = new Uint8ClampedArray(W * W * 4);
  for (let y = 0; y < W; y++) for (let x = 0; x < W; x++) {
    const v = grid[Math.floor(y / scale)][Math.floor(x / scale)] ? 0 : 255;
    const o = (y * W + x) * 4;
    px[o] = px[o + 1] = px[o + 2] = v; px[o + 3] = 255;
  }
  const decoded = jsQR(px, W, W);
  check('s24: QR decodes to the join URL', !!decoded && decoded.data === joinUrl, decoded && decoded.data);

  doc23.getElementById('phoneStopBtn').click();
  check('s24: QR hidden when the session ends', qrEl.style.display === 'none' && qrEl.innerHTML === '');

  // ---- Phone: opening the scanned URL joins, persists, and cleans the address bar ----
  const socks23p = [];
  const fetch23p = [];
  let w23p;
  const dom23p = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/?join=xyz234',
    beforeParse(win) {
      w23p = win;
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => Promise.resolve() };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => { micTrack.readyState = 'live'; micTrack.muted = false; return Promise.resolve(mockStream); }, addEventListener: () => {} };
      win.fetch = (url, opts) => {
        fetch23p.push({ url: String(url), opts: opts || {} });
        if (String(url).includes('/deliver')) {
          return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
        }
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Scan batch."}') });
      };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new win.Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks23p.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ engine: 'batch' }));
    },
  });
  await sleep(100);
  const doc23p = dom23p.window.document;
  const settings23p = () => JSON.parse(w23p.localStorage.getItem('scribe_v2_settings_v9'));

  check('s24p: scanned URL joins the session (code uppercased)', settings23p().joinedSessionCode === 'XYZ234', JSON.stringify(settings23p().joinedSessionCode));
  check('s24p: join badge + leave shown after scan', doc23p.getElementById('phoneJoinBadge').style.display !== 'none' && doc23p.getElementById('phoneLeaveBtn').style.display !== 'none');
  check('s24p: join param cleaned from the address bar', !w23p.location.search.includes('join'), w23p.location.href);
  doc23p.getElementById('apiKey').value = 'test-key';
  // The scanned join rides the next BATCH dictation: relayed via the /deliver POST.
  doc23p.getElementById('recordBtn').click();
  await sleep(80);
  check('s24p: joined phone opens no realtime transcribe WS', !socks23p.some((s) => s.url.includes('/api/transcribe')));
  doc23p.getElementById('recordBtn').click(); // stop -> upload -> deliver
  await sleep(400);
  const scanDeliver = fetch23p.find((c) => c.url.includes('/api/session/XYZ234/deliver') && c.opts && c.opts.method === 'POST' && String(c.opts.body).includes('phone_delivery'));
  check('s24p: scanned join rides the next dictation (relays to /deliver with the code)', !!scanDeliver, fetch23p.map((c) => c.url.replace('https://dictation.test', '')).join(','));
}

// ===== Scenario 25: big-button dictation layout =====
console.log('--- scenario 25: big-button layout ---');
{
  // Fresh phone-like DOM factory: batch engine (the default — no WS to drive),
  // controllable upload/deliver latency + deliver failure, and a vibration
  // spy for the haptic mirror.
  const mkBigDom = (opts) => {
    const state = {
      socks: [], fetches: [], vibes: [], win: null,
      deliverListeners: 1, deliverFail: false, deliverDelayMs: 0,
      batchText: 'Big note.', batchDelayMs: 0, batchFail: false,
      clipFail: false, // simulate iOS denying clipboard writes outside a user gesture
    };
    state.dom = new JSDOM(html, {
      runScripts: 'dangerously', url: (opts && opts.url) || 'https://dictation.test/',
      beforeParse(win) {
        state.win = win;
        win.isSecureContext = true;
        win.navigator.clipboard = { writeText: (t) => { if (state.clipFail) return Promise.reject(new Error('NotAllowedError')); win._clip = t; return Promise.resolve(); } };
        win.URL.createObjectURL = () => 'blob:mock';
        win.URL.revokeObjectURL = () => {};
        win.AudioContext = MockAudioCtx;
        win.navigator.vibrate = (p) => { state.vibes.push(p); return true; };
        // Wake lock spy: track held state so the persistent keep-awake on the
        // big-button surface (iOS mic-reclaim mitigation) can be asserted.
        state.wakeAcquires = 0; state.wakeHeld = false; state.lastWake = null;
        win.navigator.wakeLock = { request: () => { state.wakeAcquires++; state.wakeHeld = true; const s = { released: false, release() { this.released = true; state.wakeHeld = false; return Promise.resolve(); } }; state.lastWake = s; return Promise.resolve(s); } };
        // The phone surface is visible; the visibilitychange re-warm/re-lock only runs when so.
        Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
        const track = { readyState: 'live', muted: false, addEventListener() {}, stop() { this.readyState = 'ended'; } };
        const stream = { getAudioTracks: () => [track], getTracks: () => [track] };
        state.gum = 0;
        win.navigator.mediaDevices = { getUserMedia: () => { state.gum++; track.readyState = 'live'; return Promise.resolve(stream); }, addEventListener: () => {} };
        win.fetch = (url, fOpts) => {
          state.fetches.push({ url: String(url), opts: fOpts || {} });
          if (String(url).includes('/deliver')) {
            return new Promise((resolve) => setTimeout(() => resolve({
              ok: !state.deliverFail, status: state.deliverFail ? 500 : 200,
              text: () => Promise.resolve('{"ok":true,"listeners":' + state.deliverListeners + '}'),
            }), state.deliverDelayMs || 0));
          }
          return new Promise((resolve) => setTimeout(() => resolve({
            ok: !state.batchFail, status: state.batchFail ? 500 : 200,
            text: () => Promise.resolve(JSON.stringify(state.batchFail ? { error: 'upload exploded' } : { text: state.batchText })),
          }), state.batchDelayMs || 0));
        };
        win.MediaRecorder = class {
          constructor(s) { this.state = 'inactive'; }
          static isTypeSupported() { return false; }
          start() { this.state = 'recording'; }
          stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); }
        };
        const SockClass = class extends MockWS { constructor(url) { super(url); state.socks.push(this); } };
        SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
        win.WebSocket = SockClass;
        if (opts && opts.settings) win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify(opts.settings));
      },
    });
    return state;
  };
  // jsdom has no PointerEvent; a generic Event with a pointerId rides the
  // same listeners. setPointerCapture is absent (the code try/catches it),
  // so every leg runs in the same no-capture regime as capture-less
  // browsers; the slide-away leg below releases on <body>, so the
  // document-level backstop is what catches it there.
  const pev = (win, el, type, id) => {
    const ev = new win.Event(type, { bubbles: true, cancelable: true });
    ev.pointerId = id;
    el.dispatchEvent(ev);
  };

  // ---- Leg A: joining flips the layout on; Leave reverts ----
  const A = mkBigDom();
  await sleep(100);
  const dA = A.dom.window.document;
  check('s25a: normal layout before joining', !dA.body.classList.contains('bigbtn'), dA.body.className);
  dA.getElementById('phoneJoinInput').value = 'BIG123';
  dA.getElementById('phoneJoinBtn').click();
  await sleep(20);
  check('s25a: joining activates the big-button layout', dA.body.classList.contains('bigbtn'), dA.body.className);
  check('s25a: joined badge shows the code', dA.getElementById('bigJoinedBadge').textContent.includes('BIG123'), dA.getElementById('bigJoinedBadge').textContent);
  check('s25a: big Leave visible while joined', dA.getElementById('bigLeaveBtn').style.display !== 'none');
  dA.getElementById('bigLeaveBtn').click();
  await sleep(20);
  check('s25a: Leave reverts to the normal layout', !dA.body.classList.contains('bigbtn'), dA.body.className);
  check('s25a: Leave forgot the persisted join', JSON.parse(A.win.localStorage.getItem('scribe_v2_settings_v9')).joinedSessionCode === '');

  // ---- Leg B: persisted join boots into the big button + button semantics ----
  const B = mkBigDom({ settings: { joinedSessionCode: 'BIGBOOT', saveApiKey: false } });
  await sleep(100);
  const dB = B.dom.window.document;
  const bigBtnB = dB.getElementById('bigBtn');
  const screenB = () => dB.getElementById('bigUi').getAttribute('data-screen');
  const statusB = () => dB.getElementById('status').textContent;
  check('s25b: persisted join boots straight into the big button', dB.body.classList.contains('bigbtn'), dB.body.className);
  check('s25b: boot badge shows the restored code', dB.getElementById('bigJoinedBadge').textContent.includes('BIGBOOT'));
  check('s25b: screen idle at boot', screenB() === 'idle', screenB());
  dB.getElementById('apiKey').value = 'test-key';
  dB.getElementById('sonioxKey').value = 'test-skey';

  // hold = push-to-talk: pointerdown starts, pointerup past the threshold stops
  const vibesAtStart = B.vibes.length;
  pev(B.win, bigBtnB, 'pointerdown', 1);
  await sleep(150);
  check('s25b: pointerdown started the normal session path', dB.getElementById('recordBtn').textContent.includes('Stop'));
  check('s25b: no parallel session machinery (no WS in batch mode)', B.socks.length === 0, B.socks.length);
  check('s25b: whole screen shows REC', screenB() === 'rec', screenB());
  check('s25b: start haptic mirrored the start beep', JSON.stringify(B.vibes[vibesAtStart]) === '30', JSON.stringify(B.vibes.slice(vibesAtStart)));
  await sleep(450); // total hold > HOTKEY_TAP_MS
  pev(B.win, bigBtnB, 'pointerup', 1);
  await sleep(400);
  check('s25b: hold release stopped + delivered', (B.win._clip || '').includes('Big note.'), JSON.stringify(B.win._clip));
  check('s25b: success turns the screen green', screenB() === 'ok', screenB());
  check('s25b: done haptic fired (the done pattern, not just any vibe)', JSON.stringify(B.vibes[B.vibes.length - 1]) === '[40,60,40]', JSON.stringify(B.vibes.slice(vibesAtStart)));

  // Corpse-mic guard: on the big-button surface the audio graph is retained
  // between takes, but iOS can silently kill the mic track in the idle gap (it
  // still reports "live"), so the next take would record pure silence (the field
  // MIC FAIL). Finalize marks the graph suspect, so the NEXT take rebuilds from a
  // fresh getUserMedia instead of reusing the (possibly dead) track.
  const gumBeforeNextTake = B.gum;
  B.batchText = 'Tap note.';
  pev(B.win, bigBtnB, 'pointerdown', 2);
  await sleep(20);
  check('s25b: a take after a finalize rebuilds the mic graph (corpse-mic guard)', B.gum === gumBeforeNextTake + 1, (B.gum - gumBeforeNextTake) + ' getUserMedia calls');
  await sleep(80);
  pev(B.win, bigBtnB, 'pointerup', 2); // released under the tap threshold
  await sleep(200);
  check('s25b: tap keeps the recording running', dB.getElementById('recordBtn').textContent.includes('Stop'));
  pev(B.win, bigBtnB, 'pointerdown', 3); // second tap stops on press
  pev(B.win, bigBtnB, 'pointerup', 3);
  await sleep(400);
  check('s25b: second tap stopped + delivered', (B.win._clip || '').includes('Tap note.'), JSON.stringify(B.win._clip));

  // pointercancel (browser stole the pointer mid-hold) must behave as release
  B.batchText = 'Cancel note.';
  pev(B.win, bigBtnB, 'pointerdown', 4);
  await sleep(550);
  pev(B.win, bigBtnB, 'pointercancel', 4);
  await sleep(400);
  check('s25b: pointercancel never wedges the recording', (B.win._clip || '').includes('Cancel note.'), JSON.stringify(B.win._clip));

  // multi-touch: a second finger neither steals nor releases the press
  B.batchText = 'Multi note.';
  pev(B.win, bigBtnB, 'pointerdown', 5);
  await sleep(100);
  pev(B.win, bigBtnB, 'pointerdown', 6); // second finger lands
  pev(B.win, bigBtnB, 'pointerup', 6);   // and lifts
  await sleep(100);
  check('s25b: other fingers do not release the press', dB.getElementById('recordBtn').textContent.includes('Stop'));
  await sleep(350); // owning finger now past the tap threshold
  pev(B.win, bigBtnB, 'pointerup', 5);
  await sleep(400);
  check('s25b: owning finger release stops + delivers', (B.win._clip || '').includes('Multi note.'), JSON.stringify(B.win._clip));

  // peek strip: collapsed mirror, tap to expand. Append is the DESKTOP's job in
  // the phone-link flow (this device is only a mic), so click-to-append is a
  // no-op while joined and a dictation delivers a SINGLE segment — never a
  // locally-accumulated note, which would double-append on the desktop.
  const peekB = dB.getElementById('bigPeek');
  check('s25b: peek strip mirrors the latest transcript', dB.getElementById('bigPeekText').textContent.includes('Multi note.'), dB.getElementById('bigPeekText').textContent);
  check('s25b: peek starts collapsed', !peekB.classList.contains('expanded'));
  dB.getElementById('bigPeekBar').click();
  check('s25b: tapping the bar expands the peek', peekB.classList.contains('expanded'));
  dB.getElementById('bigPeekText').click(); // arming is suppressed while joined
  check('s25b: click-to-append is a no-op while joined (desktop owns append)', dB.getElementById('appendChip').style.display === 'none' && !peekB.classList.contains('armed'), dB.getElementById('appendChip').style.display);
  B.batchText = 'Single seg.';
  pev(B.win, bigBtnB, 'pointerdown', 7);
  await sleep(550);
  pev(B.win, bigBtnB, 'pointerup', 7);
  await sleep(400);
  check('s25b: a joined dictation delivers a single segment (no local accumulation)', (B.win._clip || '').includes('Single seg.') && !(B.win._clip || '').includes('Multi note.'), JSON.stringify(B.win._clip));

  // relay outcome is part of the screen: a zero-listener ack reddens it even
  // though the local delivery already succeeded (and beeped done)
  B.deliverListeners = 0;
  B.batchText = 'Down note.';
  const vibesBeforeDown = B.vibes.length;
  pev(B.win, bigBtnB, 'pointerdown', 8);
  await sleep(550);
  pev(B.win, bigBtnB, 'pointerup', 8);
  await sleep(400);
  check('s25b: local delivery still landed', (B.win._clip || '').includes('Down note.'), JSON.stringify(B.win._clip));
  check('s25b: zero-listener ack is loud', statusB().includes('Desktop link is DOWN'), statusB());
  check('s25b: zero-listener ack reddens the screen', screenB() === 'fail', screenB());
  check('s25b: warn haptic accompanied the relay warning (warn pattern, not done/fail)', JSON.stringify(B.vibes[B.vibes.length - 1]) === '[90,90,90]', JSON.stringify(B.vibes.slice(vibesBeforeDown)));
  B.deliverListeners = 1;

  // no-capture slide-away: the finger slides off the button before lifting.
  // The release lands on <body>, so the button's own listeners never see it —
  // only the document-level backstop routes it to bigBtnRelease.
  B.batchText = 'Slide note.';
  pev(B.win, bigBtnB, 'pointerdown', 9);
  await sleep(550); // past HOTKEY_TAP_MS
  pev(B.win, dB.body, 'pointerup', 9); // release off the button
  await sleep(400);
  check('s25b: slide-off release caught by the document backstop', (B.win._clip || '').includes('Slide note.'), JSON.stringify(B.win._clip));
  check('s25b: slide-off release does not wedge the screen in REC', screenB() !== 'rec', screenB());
  B.batchText = 'After slide.';
  pev(B.win, bigBtnB, 'pointerdown', 10); // the cleared pointer id must not eat the next press
  await sleep(550);
  pev(B.win, bigBtnB, 'pointerup', 10);
  await sleep(400);
  check('s25b: next press after a slide-off works normally', (B.win._clip || '').includes('After slide.'), JSON.stringify(B.win._clip));

  // pointercancel UNDER the tap threshold: the release will never arrive
  // (gesture takeover) — it must stop the dictation, not convert it to a
  // toggle that leaves the mic open.
  B.batchText = 'Cancelled tap note.';
  pev(B.win, bigBtnB, 'pointerdown', 11);
  await sleep(150); // < HOTKEY_TAP_MS
  pev(B.win, bigBtnB, 'pointercancel', 11);
  await sleep(400);
  check('s25b: sub-threshold pointercancel stops the recording', dB.getElementById('recordBtn').textContent.includes('Start'));
  check('s25b: the cancelled dictation still delivered', (B.win._clip || '').includes('Cancelled tap note.'), JSON.stringify(B.win._clip));

  // a press queued during a finalize, then cancelled, must not auto-start a
  // session nobody is holding
  B.batchDelayMs = 400;
  B.batchText = 'CDF note.';
  pev(B.win, bigBtnB, 'pointerdown', 12);
  await sleep(450);
  pev(B.win, bigBtnB, 'pointerup', 12); // hold release -> slow upload (finishing)
  await sleep(100);
  pev(B.win, bigBtnB, 'pointerdown', 13); // queued during the finalize
  await sleep(100);
  pev(B.win, bigBtnB, 'pointercancel', 13); // and cancelled before it started
  B.batchDelayMs = 0;
  await sleep(600); // delivery + would-be queued start window
  check('s25b: cancelled queued press never auto-starts', dB.getElementById('recordBtn').textContent.includes('Start'));
  check('s25b: the slow note still delivered', (B.win._clip || '').includes('CDF note.'), JSON.stringify(B.win._clip));

  // F14 (CapsLock up) while a queued start is pending cancels it — a session
  // must never start after the last F14
  B.batchDelayMs = 400;
  B.batchText = 'F14 note.';
  pev(B.win, bigBtnB, 'pointerdown', 14);
  await sleep(450);
  pev(B.win, bigBtnB, 'pointerup', 14); // slow upload (finishing)
  await sleep(100);
  dB.dispatchEvent(new B.win.KeyboardEvent('keydown', { code: 'F13' })); // queue
  dB.dispatchEvent(new B.win.KeyboardEvent('keydown', { code: 'F14' })); // CapsLock released
  B.batchDelayMs = 0;
  await sleep(600);
  check('s25b: F14 cancels a queued start', dB.getElementById('recordBtn').textContent.includes('Start'));
  check('s25b: F14-cancelled flow still delivered the note', (B.win._clip || '').includes('F14 note.'), JSON.stringify(B.win._clip));

  // hold-through-delivery ghost: press queued during a finalize, the delivery
  // lands while the finger is STILL down, the release arrives in the queued-
  // start window — it must cancel the deferred start, not be erased by it
  B.batchDelayMs = 400;
  B.deliverFail = true; // relay failure -> err outcome -> long queued-start window
  B.batchText = 'Ghost note.';
  pev(B.win, bigBtnB, 'pointerdown', 15);
  await sleep(450);
  pev(B.win, bigBtnB, 'pointerup', 15); // slow upload starts
  await sleep(100);
  pev(B.win, bigBtnB, 'pointerdown', 16); // queued; finger stays down through the delivery
  await sleep(800); // delivery + relay failure land while holding
  pev(B.win, bigBtnB, 'pointerup', 16); // held release inside the queued-start window
  await sleep(1800); // past the deferred-start delay
  check('s25b: hold released after delivery cancels the queued start (no ghost mic)', dB.getElementById('recordBtn').textContent.includes('Start'));
  check('s25b: ghost-window note still delivered locally', (B.win._clip || '').includes('Ghost note.'), JSON.stringify(B.win._clip));
  check('s25b: relay failure stayed on screen', screenB() === 'fail', screenB());

  // queued TAP + relay failure: the failure gets screen time BEFORE the
  // queued session's REC paints over it, and the queued dictation still runs
  B.batchText = 'Redden note.';
  pev(B.win, bigBtnB, 'pointerdown', 17);
  await sleep(450);
  pev(B.win, bigBtnB, 'pointerup', 17); // slow upload (batchDelayMs still 400)
  await sleep(100);
  pev(B.win, bigBtnB, 'pointerdown', 18); // quick tap during the finalize: queue survives release
  pev(B.win, bigBtnB, 'pointerup', 18);
  await sleep(700); // delivery + relay failure done; queued start still pending (err delay)
  check('s25b: relay failure shown before the queued REC repaints', screenB() === 'fail' && dB.getElementById('recordBtn').textContent.includes('Start'), screenB());
  await sleep(1600); // err-delayed queued start fires
  check('s25b: queued dictation still starts after the failure beat', dB.getElementById('recordBtn').textContent.includes('Stop'));
  B.deliverFail = false;
  B.batchDelayMs = 0;
  B.batchText = 'After redden.';
  pev(B.win, bigBtnB, 'pointerdown', 19); // tap stops the queued session
  pev(B.win, bigBtnB, 'pointerup', 19);
  await sleep(400);
  check('s25b: queued session delivered cleanly', (B.win._clip || '').includes('After redden.'), JSON.stringify(B.win._clip));

  // no-speech outcome: the sentinel lands on the clipboard — the big screen
  // must read as a FAILURE, never as a success-family DONE
  B.batchText = '';
  pev(B.win, bigBtnB, 'pointerdown', 20);
  await sleep(550);
  pev(B.win, bigBtnB, 'pointerup', 20);
  await sleep(400);
  check('s25b: no-speech outcome copies the sentinel', B.win._clip === '##DICTATION_FAILED##', JSON.stringify(B.win._clip));
  check('s25b: sentinel outcome reddens the screen', screenB() === 'fail', screenB());
  check('s25b: headline never claims DONE on a sentinel outcome', dB.getElementById('bigState').textContent === 'FAILED', dB.getElementById('bigState').textContent);
  B.batchText = 'Big note.';

  // settings stay reachable behind the existing sections
  dB.getElementById('bigSettingsBtn').click();
  check('s25b: Settings reveals the normal layout', dB.body.classList.contains('bigbtn-settings'));
  check('s25b: settings view CSS reveals the normal grid', html.includes('body.bigbtn.bigbtn-settings main > .grid { display: grid; }'));
  dB.getElementById('bigReturnBtn').click();
  check('s25b: Back returns to the button', !dB.body.classList.contains('bigbtn-settings'));

  // per-device override: "never" wins over an active join, and persists
  dB.getElementById('bigButtonMode').value = 'never';
  dB.getElementById('bigButtonMode').dispatchEvent(new B.win.Event('change', { bubbles: true }));
  check('s25b: override "never" wins over the join', !dB.body.classList.contains('bigbtn'), dB.body.className);
  await sleep(400); // debounced settings save
  check('s25b: override persisted as an additive v9 field', JSON.parse(B.win.localStorage.getItem('scribe_v2_settings_v9')).bigButtonMode === 'never');
  dB.getElementById('bigButtonMode').value = 'joined';
  dB.getElementById('bigButtonMode').dispatchEvent(new B.win.Event('change', { bubbles: true }));
  check('s25b: back to "joined" restores the layout', dB.body.classList.contains('bigbtn'));

  // batch upload failure on the big screen: the upload window must render
  // WORKING…, never a stale green DONE flash, before the red failure lands.
  B.batchFail = true;
  B.batchDelayMs = 250; // hold the upload open so the busy window is observable
  pev(B.win, bigBtnB, 'pointerdown', 21); // quick tap toggles on
  pev(B.win, bigBtnB, 'pointerup', 21);
  await sleep(120);
  pev(B.win, bigBtnB, 'pointerdown', 22); // tap off -> stop -> upload
  pev(B.win, bigBtnB, 'pointerup', 22);
  await sleep(100); // upload in flight (batchDelayMs not yet elapsed)
  check('s25b: upload window renders WORKING, not a stale success', screenB() === 'busy', screenB());
  await sleep(400); // failed upload lands
  check('s25b: failed upload lands on the red screen', screenB() === 'fail', screenB());
  check('s25b: no-text upload failure copies the sentinel', B.win._clip === '##DICTATION_FAILED##', JSON.stringify(B.win._clip));
  B.batchFail = false;
  B.batchDelayMs = 0;

  // ---- Leg C: the QR /?join= boot path lands in the big button ----
  const C = mkBigDom({ url: 'https://dictation.test/?join=btn789' });
  await sleep(100);
  const dC = C.dom.window.document;
  check('s25c: /?join= boot lands in the big-button layout', dC.body.classList.contains('bigbtn'), dC.body.className);
  check('s25c: scanned code joined + shown', dC.getElementById('bigJoinedBadge').textContent.includes('BTN789'), dC.getElementById('bigJoinedBadge').textContent);

  // ---- Leg D: override boots — "always" without a join, "never" despite one ----
  const D1 = mkBigDom({ settings: { bigButtonMode: 'always' } });
  await sleep(100);
  const dD1 = D1.dom.window.document;
  check('s25d: "always" boots into the big button with no join (solo phone)', dD1.body.classList.contains('bigbtn'), dD1.body.className);
  check('s25d: no Leave button without a join', dD1.getElementById('bigLeaveBtn').style.display === 'none');
  const D2 = mkBigDom({ settings: { bigButtonMode: 'never', joinedSessionCode: 'XYZ111' } });
  await sleep(100);
  check('s25d: persisted "never" beats a persisted join at boot', !D2.dom.window.document.body.classList.contains('bigbtn'), D2.dom.window.document.body.className);

  // ---- Leg E: joined phone whose LOCAL clipboard is denied (iOS refuses
  // writes outside a user gesture). The deliverable is the DESKTOP clipboard
  // via the relay — a denied local copy on an otherwise-clean outcome must
  // defer to the relay ack, not brand the dictation a false FAILED. ----
  const E = mkBigDom({ settings: { joinedSessionCode: 'IOSPHN' } });
  await sleep(100);
  const dE = E.dom.window.document;
  const bigBtnE = dE.getElementById('bigBtn');
  const screenE = () => dE.getElementById('bigUi').getAttribute('data-screen');
  const statusE = () => dE.getElementById('status').textContent;
  E.clipFail = true;
  dE.getElementById('apiKey').value = 'test-key';
  dE.getElementById('sonioxKey').value = 'test-skey';

  // clean dictation, desktop listening: the relay ack announces the outcome
  E.batchText = 'Relay note.';
  const vibesE1 = E.vibes.length;
  pev(E.win, bigBtnE, 'pointerdown', 1);
  await sleep(550);
  pev(E.win, bigBtnE, 'pointerup', 1);
  await sleep(400);
  check('s25e: local-copy denial with a live desktop is NOT a failure', !statusE().includes('FAILED'), statusE());
  check('s25e: relay ack announces the desktop delivery', statusE().includes('Delivered to the desktop'), statusE());
  check('s25e: screen lands green', screenE() === 'ok', screenE());
  check('s25e: done haptic closes the dictation', JSON.stringify(E.vibes[E.vibes.length - 1]) === '[40,60,40]', JSON.stringify(E.vibes.slice(vibesE1)));
  check('s25e: no fail haptic anywhere in the clean flow', !E.vibes.slice(vibesE1).some((v) => JSON.stringify(v) === '[220,90,220]'), JSON.stringify(E.vibes.slice(vibesE1)));
  check('s25e: phone clipboard genuinely untouched', E.win._clip === undefined, JSON.stringify(E.win._clip));

  // desktop gone: the zero-listener ack stays a loud red failure (no done cue)
  E.deliverListeners = 0;
  E.batchText = 'Down note.';
  const vibesE2 = E.vibes.length;
  pev(E.win, bigBtnE, 'pointerdown', 2);
  await sleep(550);
  pev(E.win, bigBtnE, 'pointerup', 2);
  await sleep(400);
  check('s25e: zero-listener ack still fails loudly', statusE().includes('Desktop link is DOWN') && screenE() === 'fail', statusE() + ' / ' + screenE());
  check('s25e: warn haptic, and never a done cue', JSON.stringify(E.vibes[E.vibes.length - 1]) === '[90,90,90]' && !E.vibes.slice(vibesE2).some((v) => JSON.stringify(v) === '[40,60,40]'), JSON.stringify(E.vibes.slice(vibesE2)));
  E.deliverListeners = 1;

  // relay hard failure: red + fail cue
  E.deliverFail = true;
  E.batchText = 'Failed relay note.';
  pev(E.win, bigBtnE, 'pointerdown', 3);
  await sleep(550);
  pev(E.win, bigBtnE, 'pointerup', 3);
  await sleep(400);
  check('s25e: relay failure stays a loud red failure', statusE().includes('Desktop relay FAILED') && screenE() === 'fail', statusE() + ' / ' + screenE());
  check('s25e: fail haptic closes the relay failure', JSON.stringify(E.vibes[E.vibes.length - 1]) === '[220,90,220]', JSON.stringify(E.vibes.slice(vibesE2)));
  E.deliverFail = false;

  // NOT joined: the local clipboard IS the deliverable again — a denied copy
  // stays the loud failure it always was
  dE.getElementById('phoneLeaveBtn').click();
  await sleep(20);
  E.batchText = 'Solo note.';
  dE.getElementById('recordBtn').click();
  await sleep(120);
  dE.getElementById('recordBtn').click();
  await sleep(400);
  check('s25e: unjoined denied copy is still a loud failure', statusE().includes('clipboard copy FAILED') && dE.getElementById('status').className.includes('err'), statusE());
  check('s25e: unjoined denied copy fail-beeps', JSON.stringify(E.vibes[E.vibes.length - 1]) === '[220,90,220]', JSON.stringify(E.vibes[E.vibes.length - 1]));

  // ---- Leg E2 (one-beep regression): a joined device whose LOCAL copy
  // SUCCEEDS (an Android phone, or a desktop joined to another desktop) must
  // STILL defer the outcome cue to the relay. A local doneBeep fired before the
  // relay would be a SECOND beep — and a doneBeep on a relay that then fails
  // would sound like success on a degraded outcome. Exactly ONE outcome cue per
  // dictation regardless of the relay result. (Outcome haptics: done=[40,60,40],
  // warn=[90,90,90], fail=[220,90,220]; the start=30 cue is filtered out.) ----
  const E2 = mkBigDom({ settings: { joinedSessionCode: 'ANDPHN' } });
  await sleep(100);
  const dE2 = E2.dom.window.document;
  const bigBtnE2 = dE2.getElementById('bigBtn');
  const screenE2 = () => dE2.getElementById('bigUi').getAttribute('data-screen');
  const statusE2 = () => dE2.getElementById('status').textContent;
  dE2.getElementById('apiKey').value = 'test-key';
  dE2.getElementById('sonioxKey').value = 'test-skey';
  const outcomes = (vibes) => vibes.filter((v) => { const s = JSON.stringify(v); return s === '[40,60,40]' || s === '[90,90,90]' || s === '[220,90,220]'; });
  // local copy SUCCEEDS here (E2.clipFail stays false) — the bug only shows when
  // the local copy did NOT already get short-circuited by an iOS denial.

  // zero-listener relay + a working local copy: ONE cue (warn), never a done
  E2.deliverListeners = 0;
  E2.batchText = 'Android down note.';
  const vE2a = E2.vibes.length;
  pev(E2.win, bigBtnE2, 'pointerdown', 1);
  await sleep(550);
  pev(E2.win, bigBtnE2, 'pointerup', 1);
  await sleep(400);
  check('s25e2: the local copy genuinely succeeded', (E2.win._clip || '').includes('Android down note.'), JSON.stringify(E2.win._clip));
  check('s25e2: zero-listener still fails loudly (relay owns the cue)', statusE2().includes('Desktop link is DOWN') && screenE2() === 'fail', statusE2() + ' / ' + screenE2());
  check('s25e2: EXACTLY ONE outcome cue and it is the warn (no local doneBeep double)', outcomes(E2.vibes.slice(vE2a)).length === 1 && JSON.stringify(outcomes(E2.vibes.slice(vE2a))[0]) === '[90,90,90]', JSON.stringify(E2.vibes.slice(vE2a)));

  // hard relay POST failure + a working local copy: ONE cue (fail), never a done
  E2.deliverListeners = 1;
  E2.deliverFail = true;
  E2.batchText = 'Android fail note.';
  const vE2b = E2.vibes.length;
  pev(E2.win, bigBtnE2, 'pointerdown', 2);
  await sleep(550);
  pev(E2.win, bigBtnE2, 'pointerup', 2);
  await sleep(400);
  check('s25e2: relay POST failure is EXACTLY ONE fail cue (no local doneBeep double)', outcomes(E2.vibes.slice(vE2b)).length === 1 && JSON.stringify(outcomes(E2.vibes.slice(vE2b))[0]) === '[220,90,220]', JSON.stringify(E2.vibes.slice(vE2b)));
  E2.deliverFail = false;

  // happy path + a working local copy: still ONE cue (done from the relay ack)
  E2.batchText = 'Android ok note.';
  const vE2c = E2.vibes.length;
  pev(E2.win, bigBtnE2, 'pointerdown', 3);
  await sleep(550);
  pev(E2.win, bigBtnE2, 'pointerup', 3);
  await sleep(400);
  check('s25e2: a delivered relay is EXACTLY ONE done cue', outcomes(E2.vibes.slice(vE2c)).length === 1 && JSON.stringify(outcomes(E2.vibes.slice(vE2c))[0]) === '[40,60,40]', JSON.stringify(E2.vibes.slice(vE2c)));
  check('s25e2: the relay ack announced the desktop delivery', statusE2().includes('Delivered to the desktop'), statusE2());

  // ---- Leg F: persistent keep-awake on the phone surface. iOS reclaims the
  // mic on screen auto-lock, so the big-button surface holds a screen wake lock
  // the WHOLE time it is up — acquired on entry, NOT released after a delivery
  // (unlike a plain desktop), re-acquired when the page returns from hidden
  // (the OS drops it on hide), and released only on Leave. This is the fix for
  // "iOS keeps killing the mic between takes". ----
  const F = mkBigDom({ settings: { joinedSessionCode: 'AWAKE1' } });
  await sleep(100);
  const dF = F.dom.window.document;
  const bigBtnF = dF.getElementById('bigBtn');
  check('s25f: wake lock acquired on entering the phone surface', F.wakeHeld === true && F.wakeAcquires >= 1, F.wakeHeld + '/' + F.wakeAcquires);
  dF.getElementById('apiKey').value = 'test-key';
  dF.getElementById('sonioxKey').value = 'test-skey';
  F.batchText = 'Awake note.';
  pev(F.win, bigBtnF, 'pointerdown', 1);
  await sleep(550);
  pev(F.win, bigBtnF, 'pointerup', 1);
  await sleep(400);
  check('s25f: dictation delivered to the desktop', F.fetches.some((f) => f.url.includes('/deliver')), JSON.stringify(F.fetches.map((f) => f.url)));
  check('s25f: wake lock STILL held after delivery (no cold mic between takes)', F.wakeHeld === true, F.wakeHeld);
  // Simulate the OS auto-releasing the lock while the page was hidden, then
  // returning to the foreground: the visibilitychange handler must re-acquire it.
  F.lastWake.released = true; F.wakeHeld = false;
  dF.dispatchEvent(new F.win.Event('visibilitychange'));
  await sleep(40);
  check('s25f: returning to the surface re-acquires the dropped lock', F.wakeHeld === true && F.lastWake.released === false, F.wakeHeld);
  // Leave reverts the layout and drops the keep-awake.
  dF.getElementById('bigLeaveBtn').click();
  await sleep(40);
  check('s25f: Leave releases the wake lock', F.wakeHeld === false && !dF.body.classList.contains('bigbtn'), F.wakeHeld + '/' + dF.body.className);
}

// ===== Scenario 26: front-and-center phone pairing (QR overlay + join handshake) =====
// The desktop's prominent "Pair a phone" button opens a big-QR overlay that
// auto-closes when a phone joins (the phone_join ping relayed through the room),
// with the first delivery as a fallback. The phone notifies the desktop on join.
console.log('--- scenario 26: phone pairing overlay ---');
{
  // ---- Desktop: "Pair a phone" opens the overlay; phone_join closes it ----
  const socks26 = [];
  const dom26 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() {} stop() {} };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks26.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc26 = dom26.window.document;
  const pairBtn = doc26.getElementById('pairPhoneBtn');
  const overlay26 = doc26.getElementById('pairOverlay');
  check('s26: a prominent "Pair a phone" button lives on the primary card (not buried in Options)', !!pairBtn && doc26.querySelector('section.card').contains(pairBtn), pairBtn ? pairBtn.textContent : 'missing');

  pairBtn.click(); // start a session + open the overlay
  await sleep(20);
  const sock26 = socks26.find((s) => s.url.includes('/api/session/'));
  check('s26: opening the overlay starts a phone session (listener socket)', !!sock26);
  const code26 = sock26 ? sock26.url.split('/api/session/')[1] : '';
  check('s26: the overlay is shown front-and-center', overlay26.classList.contains('show'));
  check('s26: the overlay renders a QR svg', !!doc26.getElementById('pairQr').querySelector('svg'));
  check('s26: the overlay shows the 6-char session code', doc26.getElementById('pairCode').textContent === code26 && code26.length === 6, doc26.getElementById('pairCode').textContent);
  sock26.open();
  await sleep(10);

  // a phone_join handshake closes the overlay and flips the button to paired
  sock26.msg({ message_type: 'phone_join' });
  await sleep(20);
  check('s26: a phone_join closes the overlay', !overlay26.classList.contains('show'));
  check('s26: the button reflects the paired state', pairBtn.textContent.toLowerCase().includes('paired'), pairBtn.textContent);
  check('s26: a join announces the pairing', doc26.getElementById('status').textContent.includes('paired'), doc26.getElementById('status').textContent);

  // re-open then End session tears it all down
  pairBtn.click();
  await sleep(10);
  check('s26: re-opening shows the overlay again', overlay26.classList.contains('show'));
  doc26.getElementById('pairEndBtn').click();
  await sleep(10);
  check('s26: End session hides the overlay', !overlay26.classList.contains('show'));
  check('s26: End session closes the room socket', sock26.closed);

  // ---- Fallback: with no phone_join, the first delivery closes the overlay ----
  const socks26b = [];
  const dom26b = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() {} stop() {} };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks26b.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc26b = dom26b.window.document;
  const overlay26b = doc26b.getElementById('pairOverlay');
  doc26b.getElementById('pairPhoneBtn').click();
  await sleep(20);
  const sock26b = socks26b.find((s) => s.url.includes('/api/session/'));
  sock26b.open();
  await sleep(10);
  check('s26b: overlay open, no join ping yet', overlay26b.classList.contains('show'));
  sock26b.msg({ message_type: 'phone_delivery', text: 'First note.', delivery_id: 'pj1' });
  await sleep(20);
  check('s26b: the first delivery closes the overlay (join-ping fallback)', !overlay26b.classList.contains('show'));

  // ---- Phone: joining POSTs a phone_join ping so the desktop overlay closes ----
  const fetch26p = [];
  const dom26p = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = (url, opts) => { fetch26p.push({ url: String(url), opts: opts || {} }); return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') }); };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() {} stop() {} };
      const SockClass = class extends MockWS { constructor(url) { super(url); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc26p = dom26p.window.document;
  doc26p.getElementById('phoneJoinInput').value = 'PAIR99';
  doc26p.getElementById('phoneJoinBtn').click();
  await sleep(20);
  const joinPing = fetch26p.find((c) => c.url.includes('/api/session/PAIR99/deliver') && c.opts && c.opts.method === 'POST' && String(c.opts.body).includes('phone_join'));
  check('s26p: joining POSTs a phone_join ping so the desktop overlay can close', !!joinPing, fetch26p.map((c) => c.url.replace('https://dictation.test', '')).join(','));
}

// ===== Scenario 26r: desktop "phone is recording" indicator =====
// A paired phone fires a relayed-but-unbuffered phone_recording ping on capture
// start/stop; the desktop reflects it as a live indicator (recording ->
// transcribing -> cleared on delivery). The phone POSTs the ping only when
// joined.
console.log('--- scenario 26r: phone-recording indicator ---');
{
  // ---- Desktop: phone_recording start/stop drives the badge; delivery clears it ----
  const socks26r = [];
  const dom26r = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() {} stop() {} };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks26r.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc26r = dom26r.window.document;
  const badge26r = doc26r.getElementById('phoneRecBadge');
  const status26r = () => doc26r.getElementById('status').textContent;
  check('s26r: the indicator lives on the primary card and starts hidden', !!badge26r && doc26r.querySelector('section.card').contains(badge26r) && badge26r.style.display === 'none', badge26r ? badge26r.style.display : 'missing');

  doc26r.getElementById('pairPhoneBtn').click(); // start a session + listener socket
  await sleep(20);
  const sock26r = socks26r.find((s) => s.url.includes('/api/session/'));
  sock26r.open();
  await sleep(10);

  sock26r.msg({ message_type: 'phone_recording', state: 'start' });
  await sleep(10);
  check('s26r: a phone_recording start shows the recording indicator', badge26r.style.display !== 'none' && badge26r.className === 'rec' && badge26r.textContent.toLowerCase().includes('recording'), badge26r.textContent + ' / ' + badge26r.className);
  check('s26r: a recording start is announced on the status line', status26r().toLowerCase().includes('recording'), status26r());

  sock26r.msg({ message_type: 'phone_recording', state: 'stop' });
  await sleep(10);
  check('s26r: a phone_recording stop flips to the transcribing state', badge26r.style.display !== 'none' && badge26r.className === 'xcribe' && badge26r.textContent.toLowerCase().includes('transcrib'), badge26r.textContent + ' / ' + badge26r.className);

  sock26r.msg({ message_type: 'phone_delivery', text: 'Indicator note.', delivery_id: 'ri1' });
  await sleep(20);
  check('s26r: the delivery clears the indicator', badge26r.style.display === 'none', badge26r.style.display);
  check('s26r: the delivered text reaches the clipboard', (dom26r.window._clip || '').includes('Indicator note.'), dom26r.window._clip);

  // A recording start that arrives before any join still pairs (acts like phone_join).
  sock26r.msg({ message_type: 'phone_recording', state: 'start' });
  await sleep(10);
  check('s26r: a recording ping proves a phone is on the link (pairs the button)', doc26r.getElementById('pairPhoneBtn').textContent.toLowerCase().includes('paired'), doc26r.getElementById('pairPhoneBtn').textContent);

  // Ending the session tears the indicator down.
  doc26r.getElementById('phoneStopBtn') && doc26r.getElementById('phoneStopBtn').click();
  await sleep(10);
  check('s26r: ending the session hides the indicator', badge26r.style.display === 'none', badge26r.style.display);

  // ---- Phone: recording POSTs phone_recording start (only when joined) ----
  const fetch26rp = [];
  const dom26rp = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = (url, opts) => {
        fetch26rp.push({ url: String(url), opts: opts || {} });
        if (String(url).includes('/deliver')) return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Phone note."}') });
      };
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new win.Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
    },
  });
  await sleep(80);
  const doc26rp = dom26rp.window.document;
  doc26rp.getElementById('phoneJoinInput').value = 'RECIND';
  doc26rp.getElementById('phoneJoinBtn').click();
  doc26rp.getElementById('apiKey').value = 'test-key';
  doc26rp.getElementById('recordBtn').click(); // start recording
  await sleep(80);
  const recStartPing = fetch26rp.find((c) => c.url.includes('/api/session/RECIND/deliver') && c.opts && String(c.opts.body).includes('phone_recording') && String(c.opts.body).includes('"start"'));
  check('s26rp: a joined phone POSTs a phone_recording start ping on capture', !!recStartPing, fetch26rp.map((c) => c.url.replace('https://dictation.test', '')).join(','));
  doc26rp.getElementById('recordBtn').click(); // stop -> upload
  await sleep(600);
  const recStopPing = fetch26rp.find((c) => c.url.includes('/api/session/RECIND/deliver') && c.opts && String(c.opts.body).includes('phone_recording') && String(c.opts.body).includes('"stop"'));
  check('s26rp: the joined phone POSTs a phone_recording stop ping when capture ends', !!recStopPing);
}

// ===== Scenario 27: mic-tips onboarding (keep other voices out) =====
// A one-time, phone-first nudge: iOS Voice Isolation steps + close-mic / push-
// to-talk discipline. Auto-shows once on the big-button surface, dismissal
// persists (no re-nag), reopenable, and the iOS-vs-Android sections switch on UA.
console.log('--- scenario 27: mic-tips onboarding ---');
{
  const mkTipsDom = (opts) => new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      if (opts && opts.ua) { try { Object.defineProperty(win.navigator, 'userAgent', { value: opts.ua, configurable: true }); } catch (e) {} }
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() {} stop() {} };
      const SockClass = class extends MockWS { constructor(url) { super(url); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify((opts && opts.settings) || {}));
    },
  });
  const IPHONE_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1';

  // iPhone, joined -> big-button -> the nudge auto-shows with the iOS steps
  const dom27 = mkTipsDom({ ua: IPHONE_UA, settings: { joinedSessionCode: 'TIPS01' } });
  await sleep(120);
  const doc27 = dom27.window.document;
  const tips27 = doc27.getElementById('micTips');
  check('s27: big-button layout active for a joined phone', doc27.body.classList.contains('bigbtn'));
  check('s27: mic-tips auto-shows on the phone surface', tips27.classList.contains('show'));
  check('s27: iOS Voice Isolation steps shown on iPhone', doc27.getElementById('micTipsIos').style.display !== 'none');
  check('s27: Android note hidden on iPhone', doc27.getElementById('micTipsAndroid').style.display === 'none');

  doc27.getElementById('micTipsDoneBtn').click();
  check('s27: "Got it" hides the nudge', !tips27.classList.contains('show'));
  const settings27 = JSON.parse(dom27.window.localStorage.getItem('scribe_v2_settings_v9'));
  check('s27: dismissal persists micTipsSeen (per-device)', settings27.micTipsSeen === true, JSON.stringify(settings27.micTipsSeen));

  doc27.getElementById('bigTipsBtn').click();
  check('s27: the "Mic tips" button reopens the nudge', tips27.classList.contains('show'));
  // backdrop tap dismisses
  tips27.dispatchEvent(new dom27.window.MouseEvent('click', { bubbles: true }));
  check('s27: tapping the backdrop dismisses it', !tips27.classList.contains('show'));

  // A dismissed nudge must NOT auto-show again on the next load
  const dom27b = mkTipsDom({ ua: IPHONE_UA, settings: { joinedSessionCode: 'TIPS01', micTipsSeen: true } });
  await sleep(120);
  check('s27: a dismissed nudge does not auto-show again', !dom27b.window.document.getElementById('micTips').classList.contains('show'));

  // A plain desktop (not joined, not big-button) must NOT auto-show the nudge
  const dom27c = mkTipsDom({ settings: {} });
  await sleep(120);
  check('s27: no auto-show on a plain desktop', !dom27c.window.document.getElementById('micTips').classList.contains('show'));

  // Non-iOS device: reopening shows the Android note, hides the iOS steps
  const dom27d = mkTipsDom({ settings: { bigButtonMode: 'always', micTipsSeen: true } }); // big-button active, but seen -> no auto-show
  await sleep(120);
  const doc27d = dom27d.window.document;
  doc27d.getElementById('bigTipsBtn').click();
  check('s27: non-iOS hides the iOS steps', doc27d.getElementById('micTipsIos').style.display === 'none');
  check('s27: non-iOS shows the Android note', doc27d.getElementById('micTipsAndroid').style.display !== 'none');
}

// ===== Scenario 29: batch-only product (engine migration + capture feedback) =====
console.log('--- scenario 29: batch-only product ---');
{
  const socks29 = [];
  let w29;
  const dom29 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      w29 = win;
      win.isSecureContext = true;
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, muted: false, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Batch note."}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new win.Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      const SockClass = class extends MockWS { constructor(url) { super(url); socks29.push(this); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ engine: 'hybrid' })); // a pre-existing hybrid user
    },
  });
  await sleep(100);
  const doc29 = dom29.window.document;
  const settings29 = () => JSON.parse(w29.localStorage.getItem('scribe_v2_settings_v9'));

  // The engine selector is gone (batch-only); the dictation below proves the
  // product behaves as batch regardless of the saved Hybrid engine.
  check('s29: no engine selector in the DOM (batch-only)', !doc29.getElementById('engineSeg') && !doc29.getElementById('engRealtime'));

  doc29.getElementById('apiKey').value = 'test-key';
  doc29.getElementById('recordBtn').click();
  await sleep(140); // let the gate-meter loop tick and reveal the capture feedback
  check('s29: recording shows the live capture feedback (waveform + timer)', doc29.getElementById('recFeedback').style.display !== 'none');
  check('s29: no realtime WebSocket opens in batch mode', !socks29.some((s) => s.url.includes('/api/transcribe')));
  doc29.getElementById('recordBtn').click(); // stop -> upload -> deliver
  await sleep(140);
  check('s29: capture feedback hides once recording ends', doc29.getElementById('recFeedback').style.display === 'none');
  check('s29: batch text reaches the clipboard', (w29._clip || '').includes('Batch note'), w29._clip);
  // The saved Hybrid engine migrated to Batch: the history entry is tagged batch
  // and the persisted engine is batch.
  const hist29 = JSON.parse(w29.localStorage.getItem('scribe_v2_transcripts_v9') || '[]');
  check('s29: a saved Hybrid engine migrates to Batch (history tagged batch)', hist29[0] && hist29[0].engine === 'batch', hist29[0] && hist29[0].engine);
  check('s29: the migrated engine persists as batch', settings29().engine === 'batch', JSON.stringify(settings29().engine));
}

// ===== Scenario 30: diarization — keep-primary-speaker filters bystander voices =====
// Background voices the mic catches land as a SECOND Scribe speaker. The
// (default-on, per-device) "Filter out other speakers" toggle keeps only the
// dominant speaker — but ONLY on the phone / big-button surface (a plain desktop
// keeps the proven single-speaker config), ONLY when one speaker clearly
// dominates (the minority guard), and a LARGE removal is a degraded warn (not a
// clean Done!) with the unfiltered text stashed in history for recovery.
console.log('--- scenario 30: diarization keep-primary-speaker ---');
doc.getElementById('apiKey').value = 'test-key';
doc.getElementById('freshBtn').click();
check('s30: diarize toggle defaults ON (per-device)', doc.getElementById('diarize').checked === true);
const mkWords = (spec) => spec.flatMap(([sid, ...ws]) => ws.map((t) => ({ text: t, type: 'word', speaker_id: sid })));
const hist0 = () => JSON.parse(w.localStorage.getItem('scribe_v2_transcripts_v9') || '[]')[0] || {};

// Leg 0: DESKTOP surface (default, not joined) — even with the toggle ON,
// diarization is SUPPRESSED: diarize=false is sent and the full text is
// delivered. The desktop keeps the proven single-speaker path.
const wordsTwo = mkWords([['speaker_0', 'Patient', 'is', 'stable'], ['speaker_1', 'hey', 'grab', 'lunch'], ['speaker_0', 'today']]);
fetchQueue.push({ status: 200, body: { text: 'Patient is stable hey grab lunch today', words: wordsTwo } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
const up0 = fetchCalls[fetchCalls.length - 1];
check('s30: desktop surface SUPPRESSES diarization (diarize=false even with toggle on)', up0.form.get('diarize') === 'false', up0.form.get('diarize'));
check('s30: desktop surface delivers the full unfiltered text', clipboard.includes('lunch'), JSON.stringify(clipboard));

// Switch to the phone / big-button surface so diarization actually applies.
doc.getElementById('bigButtonMode').value = 'always';
doc.getElementById('bigButtonMode').dispatchEvent(new w.Event('change', { bubbles: true }));

// Leg A: a CLEAR minority bystander (1 of 9 words) — small removal, clean Done!.
doc.getElementById('freshBtn').click();
const wordsA = mkWords([['speaker_0', 'Patient', 'is', 'stable', 'and', 'resting', 'comfortably', 'this', 'morning'], ['speaker_1', 'okay']]);
fetchQueue.push({ status: 200, body: { text: 'Patient is stable and resting comfortably this morning okay', words: wordsA } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
const upA = fetchCalls[fetchCalls.length - 1];
check('s30: phone surface sends diarize=true', upA.form.get('diarize') === 'true', upA.form.get('diarize'));
check('s30: primary speaker kept on the clipboard', clipboard.includes('Patient is stable and resting comfortably this morning'), JSON.stringify(clipboard));
check('s30: clear-minority bystander dropped', !clipboard.includes('okay'), JSON.stringify(clipboard));
check('s30: small removal stays a clean success (Done!)', status().includes('Done!') && statusCls().includes('ok'), status());
check('s30: small removal reports the count', status().includes('Filtered out 1 word from other speakers'), status());

// Leg A2: a LARGE removal (3 of 9 words) — degraded WARN, not Done!, and the
// unfiltered transcript is stashed in history for recovery.
doc.getElementById('freshBtn').click();
const wordsBig = mkWords([['speaker_0', 'Assessment', 'and', 'plan', 'continue', 'current', 'medications'], ['speaker_1', 'did', 'you', 'eat']]);
fetchQueue.push({ status: 200, body: { text: 'Assessment and plan continue current medications did you eat', words: wordsBig } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s30: large removal kept the dominant speaker', clipboard.includes('Assessment and plan continue current medications'), JSON.stringify(clipboard));
check('s30: large removal dropped the minority speaker', !clipboard.includes('did you eat'), JSON.stringify(clipboard));
check('s30: large removal is a degraded WARN (not Done!)', statusCls().includes('warn') && !status().includes('Done!'), status() + ' / ' + statusCls());
check('s30: large removal status says VERIFY', status().includes('VERIFY'), status());
check('s30: unfiltered transcript stashed in history for recovery', (hist0().unfiltered || '').includes('did you eat'), JSON.stringify(hist0()));

// Leg guard: NO clear majority (4 vs 3 = 57%, below the 60% min share) — the
// filter keeps EVERYTHING rather than risk dropping the clinician (or acting on
// Scribe over-diarizing one continuous voice).
doc.getElementById('freshBtn').click();
const wordsEven = mkWords([['speaker_0', 'the', 'patient', 'reports', 'mild'], ['speaker_1', 'pain', 'since', 'yesterday']]);
fetchQueue.push({ status: 200, body: { text: 'the patient reports mild pain since yesterday', words: wordsEven } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s30: minority guard keeps BOTH speakers when none dominates', clipboard.includes('patient reports mild') && clipboard.includes('pain since yesterday'), JSON.stringify(clipboard));
check('s30: minority guard => no removal note, clean Done!', !status().includes('Filtered out') && status().includes('Done!'), status());

// Leg inverted: a talkative BYSTANDER out-talks the clinician (6 vs 2). The
// most-words heuristic keeps the wrong speaker — but the safety net makes it
// LOUD (degraded warn) and recoverable (unfiltered in history), never a silent
// confident Done!.
doc.getElementById('freshBtn').click();
const wordsInv = mkWords([['speaker_1', 'so', 'anyway', 'i', 'told', 'her', 'that'], ['speaker_0', 'chest', 'pain']]);
fetchQueue.push({ status: 200, body: { text: 'so anyway i told her that chest pain', words: wordsInv } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s30: inverted dominance is a degraded WARN, never a confident Done!', statusCls().includes('warn') && !status().includes('Done!'), status() + ' / ' + statusCls());
check('s30: inverted dominance keeps the clinician words recoverable in history', (hist0().unfiltered || '').includes('chest pain'), JSON.stringify(hist0()));

// Leg B: a single speaker is a no-op — the full text survives, no note.
doc.getElementById('freshBtn').click();
const wordsOne = mkWords([['speaker_0', 'Solo', 'note', 'here']]);
fetchQueue.push({ status: 200, body: { text: 'Solo note here', words: wordsOne } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
check('s30: single-speaker note delivered intact', clipboard.includes('Solo note here'), JSON.stringify(clipboard));
check('s30: single speaker => no removal note', !status().includes('Filtered out'), status());

// Leg C: toggle OFF — diarize=false sent, and the client never filters even if
// the server were to return a multi-speaker words[] (the guard is client-side).
doc.getElementById('freshBtn').click();
doc.getElementById('diarize').checked = false;
doc.getElementById('diarize').dispatchEvent(new w.Event('change'));
fetchQueue.push({ status: 200, body: { text: 'Patient is stable hey grab lunch today', words: wordsTwo } });
doc.getElementById('recordBtn').click();
await sleep(120);
doc.getElementById('recordBtn').click();
await sleep(300);
const upC = fetchCalls[fetchCalls.length - 1];
check('s30: upload carries diarize=false when the toggle is off', upC.form.get('diarize') === 'false', upC.form.get('diarize'));
check('s30: toggle off => no client-side filtering (full text delivered)', clipboard.includes('lunch'), JSON.stringify(clipboard));
check('s30: toggle off => no removal note', !status().includes('Filtered out'), status());
check('s30: diarize=false persisted (per-device)', JSON.parse(w.localStorage.getItem('scribe_v2_settings_v9')).diarize === false, w.localStorage.getItem('scribe_v2_settings_v9'));
doc.getElementById('diarize').checked = true; // restore default for any later reuse
doc.getElementById('diarize').dispatchEvent(new w.Event('change'));
doc.getElementById('bigButtonMode').value = 'joined'; // leave the surface as it was
doc.getElementById('bigButtonMode').dispatchEvent(new w.Event('change', { bubbles: true }));

// ===== Scenario 31m: mic capture levers + silent-capture telemetry =====
// The per-device echo-cancellation / makeup-gain / auto-gain levers that the
// silent-capture commits added (#39/#42) had no coverage. Assert their defaults,
// that toggling the capture constraints rebuilds the graph (and rides into
// getUserMedia), that makeup gain updates live without a rebuild, that the
// levers persist + reload, the failure log is captured — and the CENTRAL #42
// safety invariant: makeup gain (pre-analyser) can NEVER mask a dead mic.
console.log('--- scenario 31m: mic levers + silent-capture telemetry ---');
{
  doc.getElementById('freshBtn').click();
  const ec = doc.getElementById('echoCancel');
  const ag = doc.getElementById('autoGain');
  const mg = doc.getElementById('micGain');

  check('s31m: echo cancellation defaults ON', ec.checked === true);
  check('s31m: makeup gain defaults to 1x', mg.value === '1', mg.value);
  check('s31m: auto gain control defaults OFF', ag.checked === false);
  check('s31m: Copy button is labelled "Copy & clear"', doc.getElementById('copyBtn').textContent.includes('Copy & clear'), doc.getElementById('copyBtn').textContent);

  // Echo cancellation is a capture constraint -> toggling it rebuilds the graph.
  let g0 = gumCalls;
  ec.checked = false; ec.dispatchEvent(new w.Event('change'));
  await sleep(200);
  check('s31m: toggling echo cancellation rebuilds the audio graph', gumCalls > g0, gumCalls + ' vs ' + g0);
  check('s31m: rebuild carries echoCancellation:false into getUserMedia', !!(lastGumConstraints && lastGumConstraints.audio && lastGumConstraints.audio.echoCancellation === false), JSON.stringify(lastGumConstraints && lastGumConstraints.audio));
  ec.checked = true; ec.dispatchEvent(new w.Event('change')); await sleep(200);

  // Auto gain control rebuilds too.
  g0 = gumCalls;
  ag.checked = true; ag.dispatchEvent(new w.Event('change'));
  await sleep(200);
  check('s31m: toggling auto gain rebuilds the audio graph', gumCalls > g0, gumCalls + ' vs ' + g0);
  check('s31m: rebuild carries autoGainControl:true into getUserMedia', !!(lastGumConstraints && lastGumConstraints.audio.autoGainControl === true), JSON.stringify(lastGumConstraints && lastGumConstraints.audio));
  ag.checked = false; ag.dispatchEvent(new w.Event('change')); await sleep(200);

  // Makeup gain is a live node, NOT a capture constraint -> no rebuild.
  g0 = gumCalls;
  mg.value = '8'; mg.dispatchEvent(new w.Event('input'));
  await sleep(60);
  check('s31m: makeup gain updates live without a graph rebuild', gumCalls === g0, gumCalls + ' vs ' + g0);
  await sleep(300); // let the debounced save flush
  const saved = JSON.parse(w.localStorage.getItem('scribe_v2_settings_v9'));
  check('s31m: levers persist (echoCancel on, autoGain off, micGain 8)', saved.echoCancel === true && saved.autoGain === false && saved.micGain === '8', JSON.stringify({ ec: saved.echoCancel, ag: saved.autoGain, mg: saved.micGain }));

  // CRITICAL invariant (#42): makeup gain sits BEFORE the analyser, so gain × 0
  // = 0 — a high gain must NEVER lift a dead mic's silence above the flatline
  // watchdog. A 15x flatline still alarms and copies the sentinel.
  mg.value = '15'; mg.dispatchEvent(new w.Event('input'));
  doc.getElementById('freshBtn').click();
  micRms = 0.0; // dead mic
  doc.getElementById('recordBtn').click();
  await sleep(2900);
  check('s31m: high makeup gain does NOT mask a dead mic (alarm still fires)', status().includes('MIC NOT CAPTURING'), status());
  check('s31m: dead-mic diagnostic shows the gain that was set', status().includes('gain:15.0x'), status());
  fetchQueue.push({ status: 200, body: { text: '' } });
  doc.getElementById('recordBtn').click();
  await sleep(300);
  check('s31m: dead mic at high gain still copies the sentinel', clipboard === '##DICTATION_FAILED##', JSON.stringify(clipboard));
  const flog = JSON.parse(w.localStorage.getItem('scribe_v2_micfail_v9') || '[]');
  check('s31m: the silent-capture failure was logged with the audio-graph state',
    flog.some((e) => /no-capture-flatline|no-speech-mic-alarm/.test(e.reason) && (e.diag || '').includes('gain:15.0x')),
    JSON.stringify(flog.slice(0, 2)));
  micRms = 0.05; mg.value = '1'; mg.dispatchEvent(new w.Event('input'));

  // micDiag on the QUICK-RELEASE no-speech path (#41): a short tap that never
  // trips the 2.5s flatline watchdog still surfaces + logs the audio-graph state.
  doc.getElementById('freshBtn').click();
  fetchQueue.push({ status: 200, body: { text: '' } });
  doc.getElementById('recordBtn').click();
  await sleep(80);
  doc.getElementById('recordBtn').click();
  await sleep(300);
  check('s31m: quick-release no-speech copies the sentinel', clipboard === '##DICTATION_FAILED##', JSON.stringify(clipboard));
  check('s31m: quick-release no-speech surfaces the audio-graph diagnostic', status().includes('ec:') && status().includes('gain:') && status().includes('peak:'), status());
  const flog2 = JSON.parse(w.localStorage.getItem('scribe_v2_micfail_v9') || '[]');
  check('s31m: quick-release no-speech logged with its reason', !!(flog2[0] && flog2[0].reason === 'no-speech-quick-release'), JSON.stringify(flog2[0]));

  // Reload round-trip: a fresh boot reads the persisted levers via loadSettings.
  const rDom = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.AudioContext = MockAudioCtx; win.WebSocket = MockWS;
      win.MediaRecorder = class { constructor() { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      win.URL.createObjectURL = () => 'blob:mock'; win.URL.revokeObjectURL = () => {};
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{}') });
      const t = { readyState: 'live', muted: false, addEventListener() {}, stop() { this.readyState = 'ended'; } };
      const st = { getAudioTracks: () => [t], getTracks: () => [t] };
      Object.defineProperty(win.navigator, 'mediaDevices', { value: { getUserMedia: () => Promise.resolve(st), addEventListener() {} }, configurable: true });
      Object.defineProperty(win.navigator, 'permissions', { value: { query: () => Promise.resolve({ state: 'granted' }) }, configurable: true });
      Object.defineProperty(win.navigator, 'clipboard', { value: { writeText() { return Promise.resolve(); } }, configurable: true });
      Object.defineProperty(win, 'isSecureContext', { value: true, configurable: true });
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ saveApiKey: true, echoCancel: false, micGain: '9', autoGain: true }));
      win.addEventListener('error', (e) => { console.log('PAGE ERROR (reload):', e.message); failures++; });
    },
  });
  await sleep(150);
  const rd = rDom.window.document;
  check('s31m: reload restores echoCancel=false via loadSettings', rd.getElementById('echoCancel').checked === false);
  check('s31m: reload restores micGain=9 via loadSettings', rd.getElementById('micGain').value === '9', rd.getElementById('micGain').value);
  check('s31m: reload restores autoGain=true via loadSettings', rd.getElementById('autoGain').checked === true);
}

// ===== Scenario 32j: dictation journal (crash-safe audio recovery) =====
// The roadmap's #1 reliability gap: a device that captures audio but dies before
// upload+delivery used to silently lose that take. The journal mirrors each
// chunk to IndexedDB during recording; the next boot finds an un-finalized take
// and offers a loud recovery (re-upload + deliver). A normal finish clears it.
console.log('--- scenario 32j: dictation journal crash recovery ---');
{
  // One device's persistent IndexedDB, SHARED across the "crash" and the "reboot"
  // so the journal survives the simulated process death.
  const sharedIDB = new IDBFactory();
  const mkJDom = (opts) => {
    const st = { fetches: [], batchText: 'Recovered words.', batchFail: false, win: null };
    st.dom = new JSDOM(html, {
      runScripts: 'dangerously', url: 'https://dictation.test/',
      beforeParse(win) {
        st.win = win;
        win.AudioContext = MockAudioCtx; win.WebSocket = MockWS;
        win.indexedDB = sharedIDB;
        // MediaRecorder that mirrors start(1000): emit chunks on a short interval
        // so chunks actually land in the journal during "recording".
        win.MediaRecorder = class {
          constructor() { this.state = 'inactive'; this._iv = null; }
          static isTypeSupported() { return false; }
          start() { this.state = 'recording'; this._iv = win.setInterval(() => { if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); }, 40); }
          stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this._iv) { win.clearInterval(this._iv); this._iv = null; } if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); }
        };
        win.URL.createObjectURL = () => 'blob:mock'; win.URL.revokeObjectURL = () => {};
        win.fetch = (url) => { st.fetches.push(String(url)); return new Promise((resolve) => setTimeout(() => resolve({ ok: !st.batchFail, status: st.batchFail ? 500 : 200, text: () => Promise.resolve(JSON.stringify(st.batchFail ? { error: 'x' } : { text: st.batchText })) }), 5)); };
        const t = { readyState: 'live', muted: false, addEventListener() {}, stop() { this.readyState = 'ended'; } };
        const stream = { getAudioTracks: () => [t], getTracks: () => [t] };
        Object.defineProperty(win.navigator, 'mediaDevices', { value: { getUserMedia: () => Promise.resolve(stream), addEventListener() {} }, configurable: true });
        Object.defineProperty(win.navigator, 'permissions', { value: { query: () => Promise.resolve({ state: 'granted' }) }, configurable: true });
        Object.defineProperty(win.navigator, 'clipboard', { value: { writeText: (x) => { win._clip = x; return Promise.resolve(); } }, configurable: true });
        Object.defineProperty(win, 'isSecureContext', { value: true, configurable: true });
        if (opts && opts.settings) win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify(opts.settings));
        win.addEventListener('error', (e) => { console.log('PAGE ERROR (32j):', e.message); failures++; });
      },
    });
    return st;
  };
  const orphanRecordingCount = () => new Promise((resolve) => {
    const req = sharedIDB.open('scribe_v2_journal', 1);
    req.onsuccess = () => { const db = req.result; const g = db.transaction('sessions', 'readonly').objectStore('sessions').getAll(); g.onsuccess = () => { db.close(); resolve((g.result || []).filter((s) => s.state === 'recording').length); }; g.onerror = () => { db.close(); resolve(-1); }; };
    req.onerror = () => resolve(-1);
  });

  // ---- crash: capture audio, never finalize ----
  const crash = mkJDom({ settings: { saveApiKey: true } });
  await sleep(150);
  const cd = crash.dom.window.document;
  cd.getElementById('apiKey').value = 'k';
  cd.getElementById('recordBtn').click(); // start -> chunks mirror to the journal on the interval
  await sleep(220);                        // ~5 chunks journaled
  crash.dom.window.close();                // simulate the page dying before stop/finalize
  check('s32j: an interrupted take left an orphan in the journal', (await orphanRecordingCount()) >= 1, await orphanRecordingCount());

  // ---- reboot: same device IndexedDB, boot finds + offers recovery ----
  const reboot = mkJDom({ settings: { saveApiKey: true } });
  reboot.batchText = 'Recovered dictation text.';
  await sleep(300); // boot + restoreJournal (async)
  const rd = reboot.dom.window.document;
  check('s32j: reboot surfaces the recovery banner', rd.getElementById('journalRecover').style.display !== 'none', rd.getElementById('journalRecover').style.display);
  check('s32j: recovery banner names it as interrupted', rd.getElementById('journalRecoverMsg').textContent.includes('interrupted'), rd.getElementById('journalRecoverMsg').textContent);
  rd.getElementById('apiKey').value = 'k';
  rd.getElementById('journalRecoverBtn').click(); // recover -> re-upload -> deliver locally
  await sleep(350);
  check('s32j: recovered audio is re-uploaded for transcription', reboot.fetches.some((u) => u.includes('/api/transcribe')), reboot.fetches.join(','));
  check('s32j: recovered text lands on the clipboard', (reboot.win._clip || '').includes('Recovered dictation text.'), JSON.stringify(reboot.win._clip));
  check('s32j: recovered note is in the box', rd.getElementById('latest').textContent.includes('Recovered dictation text.'), rd.getElementById('latest').textContent);
  check('s32j: recovery banner hidden after success', rd.getElementById('journalRecover').style.display === 'none');
  check('s32j: journal cleared after successful recovery', (await orphanRecordingCount()) === 0, await orphanRecordingCount());
  reboot.dom.window.close();

  // ---- happy path: a normally finished take leaves NO orphan ----
  const clean = mkJDom({ settings: { saveApiKey: true } });
  clean.batchText = 'Clean take.';
  await sleep(250);
  const cld = clean.dom.window.document;
  cld.getElementById('apiKey').value = 'k';
  cld.getElementById('recordBtn').click();
  await sleep(150);
  cld.getElementById('recordBtn').click(); // stop -> upload -> deliver -> journalFinish clears it
  await sleep(350);
  check('s32j: a normally finished take leaves no orphan', (await orphanRecordingCount()) === 0, await orphanRecordingCount());

  // ---- discard path: a fresh boot with the clean DB shows no banner ----
  const reboot2 = mkJDom({ settings: { saveApiKey: true } });
  await sleep(300);
  check('s32j: no banner when there is nothing to recover', reboot2.dom.window.document.getElementById('journalRecover').style.display === 'none');
  clean.dom.window.close();
  reboot2.dom.window.close();
}

// ===== Scenario 33: iOS quiet-mic level seed (foolproof out-of-the-box gain) =====
// A quiet iPhone mic records below the load-bearing gate (the silent dead-band).
// An additive, iOS-gated, ONE-SHOT seed raises a MODEST makeup gain + lowers the
// gate so a normal voice clears it without anyone opening Advanced — never on a
// hand-tuned device, never on desktop, versioned (no _v9 bump), and the gain cap
// stays at/below the dead-mic-mask boundary so a dead mic still reads 0.
console.log('--- scenario 33: iOS quiet-mic level seed ---');
{
  const IPHONE_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1';
  const DESKTOP_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36';
  const mkSeedDom = (opts) => new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      if (opts && opts.ua) { try { Object.defineProperty(win.navigator, 'userAgent', { value: opts.ua, configurable: true }); } catch (e) {} }
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock';
      win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => Promise.resolve({ getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }], getAudioTracks: () => [{ readyState: 'live', enabled: true, stop() {}, addEventListener() {} }] }), addEventListener: () => {} };
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
      win.MediaRecorder = class { constructor(s) { this.state = 'inactive'; } static isTypeSupported() { return false; } start() {} stop() {} };
      const SockClass = class extends MockWS { constructor(url) { super(url); } };
      SockClass.CONNECTING = 0; SockClass.OPEN = 1; SockClass.CLOSING = 2; SockClass.CLOSED = 3;
      win.WebSocket = SockClass;
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify((opts && opts.settings) || {}));
    },
  });

  // (a) fresh iPhone, no saved audio fields -> seeded exactly once
  const d33a = mkSeedDom({ ua: IPHONE_UA, settings: {} });
  await sleep(120);
  const doc33a = d33a.window.document;
  check('s33: fresh iPhone seeds a modest makeup gain (3x)', doc33a.getElementById('micGain').value === '3', doc33a.getElementById('micGain').value);
  check('s33: seeded gain stays at/below the dead-mic-mask boundary (<=4x)', Number(doc33a.getElementById('micGain').value) <= 4, doc33a.getElementById('micGain').value);
  check('s33: fresh iPhone lowers the gate so a normal voice clears it (0.018)', doc33a.getElementById('gateOpen').value === '0.018', doc33a.getElementById('gateOpen').value);
  check('s33: seeded gate stays above gateClose 0.008', Number(doc33a.getElementById('gateOpen').value) > Number(doc33a.getElementById('gateClose').value), doc33a.getElementById('gateOpen').value + ' vs ' + doc33a.getElementById('gateClose').value);
  const s33a = JSON.parse(d33a.window.localStorage.getItem('scribe_v2_settings_v9'));
  check('s33: the seed version is stamped (one-shot)', s33a.audioSeedVersion === 1, JSON.stringify(s33a.audioSeedVersion));

  // (b) already seeded at the current version -> NOT re-seeded (the saved value wins)
  const d33b = mkSeedDom({ ua: IPHONE_UA, settings: { audioSeedVersion: 1, micGain: '2' } });
  await sleep(120);
  check('s33: an already-seeded phone is not re-seeded', d33b.window.document.getElementById('micGain').value === '2', d33b.window.document.getElementById('micGain').value);

  // (c) hand-tuned device -> the seed must never overwrite it
  const d33c = mkSeedDom({ ua: IPHONE_UA, settings: { audioUserTuned: true } });
  await sleep(120);
  check('s33: a hand-tuned iPhone is left alone (gain unchanged)', d33c.window.document.getElementById('micGain').value === '1', d33c.window.document.getElementById('micGain').value);
  check('s33: a hand-tuned iPhone is left alone (gate unchanged)', d33c.window.document.getElementById('gateOpen').value === '0.030', d33c.window.document.getElementById('gateOpen').value);

  // (d) desktop UA -> never seeded (the iOS gate excludes it)
  const d33d = mkSeedDom({ ua: DESKTOP_UA, settings: {} });
  await sleep(120);
  check('s33: desktop is never seeded (gain unchanged)', d33d.window.document.getElementById('micGain').value === '1', d33d.window.document.getElementById('micGain').value);
  check('s33: desktop is never seeded (gate unchanged)', d33d.window.document.getElementById('gateOpen').value === '0.030', d33d.window.document.getElementById('gateOpen').value);
}

// ===== Scenario 23p: phone corpse-mic probe (press-path mic health check) =====
// On the big-button surface the mic can be an iOS corpse (track reports
// "live"/unmuted but delivers pure silence after a no-event reclaim) — a REUSED
// graph or even a FRESH one (the VPIO wall). EVERY press probes the analyser
// with a short settle window: a dead read forces one fresh getUserMedia, and a
// mic that is STILL dead fails LOUD **before capture** — REC never starts on a
// provably silent mic, so the clinician cannot dictate a take into it. A
// healthy mic shows a floor on frame 0, so a normal press pays nothing.
// micGranted -> the graph warms at boot (audioSuspect false), so the FIRST
// press REUSES it (the classic corpse-risk path).
console.log('--- scenario 23p: phone corpse-mic probe ---');
const mkProbeDom = (onGum, settings) => new JSDOM(html, {
  runScripts: 'dangerously', url: 'https://dictation.test/',
  beforeParse(win) {
    win.isSecureContext = true;
    Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
    win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
    win.URL.createObjectURL = () => 'blob:mock'; win.URL.revokeObjectURL = () => {};
    win.AudioContext = MockAudioCtx;
    win.navigator.mediaDevices = { getUserMedia: () => { onGum(); return Promise.resolve({ getAudioTracks: () => [{ readyState: 'live', muted: false, enabled: true, stop() {}, addEventListener() {} }], getTracks: () => [{ readyState: 'live', stop() {}, addEventListener() {} }] }); }, addEventListener: () => {} };
    win._fetches = 0;
    win.fetch = () => { win._fetches++; return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":""}') }); }; // empty transcript -> the loud no-signal finalize
    win._recStarts = 0;
    win.MediaRecorder = class { constructor() { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { win._recStarts++; this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
    const Sock = class extends MockWS {}; Sock.CONNECTING = 0; Sock.OPEN = 1; Sock.CLOSING = 2; Sock.CLOSED = 3; win.WebSocket = Sock;
    win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify(settings || { micGranted: true, bigButtonMode: 'always' }));
  },
});
{
  // (a) corpse mic (analyser flat zero) on a REUSED graph -> the probe settles
  //     (~400 ms), forces ONE pre-capture rebuild, re-probes, and a still-dead
  //     mic fails LOUD **before capture**: no REC, no upload, sentinel copied.
  let gumA = 0;
  const domA = mkProbeDom(() => { gumA++; });
  await sleep(140); // boot: micGranted warms the graph once (audioSuspect cleared)
  const docA = domA.window.document;
  docA.getElementById('apiKey').value = 'test-key';
  micRms = 0.0; // corpse: the analyser reads exact zeros
  const gumA0 = gumA;
  const fetchesA0 = domA.window._fetches;
  docA.getElementById('recordBtn').click(); // press -> probe settles -> rebuild -> re-probe -> loud pre-capture fail
  await sleep(1300); // two settle windows (2 x 400 ms) + slack
  check('s23p: a corpse mic forces exactly one pre-capture rebuild', gumA === gumA0 + 1, 'gum delta ' + (gumA - gumA0));
  check('s23p: a still-dead mic fails LOUD before capture (MIC NOT CAPTURING, no REC)', docA.getElementById('status').textContent.includes('MIC NOT CAPTURING') && docA.getElementById('status').textContent.includes('did NOT start'), docA.getElementById('status').textContent);
  check('s23p: the recorder never started on the dead mic', domA.window._recStarts === 0, 'recStarts ' + domA.window._recStarts);
  check('s23p: nothing was uploaded for the dead press', domA.window._fetches === fetchesA0, 'fetches delta ' + (domA.window._fetches - fetchesA0));
  check('s23p: the dead press copies the sentinel', domA.window._clip === '##DICTATION_FAILED##', JSON.stringify(domA.window._clip));
  check('s23p: the dead press is a fail status', docA.getElementById('status').className.includes('err'), docA.getElementById('status').className);

  // (b) a healthy mic always shows a floor -> the probe exits on frame 0 (NO
  //     extra getUserMedia, no settle wait), so a normal press pays nothing.
  let gumB = 0;
  const domB = mkProbeDom(() => { gumB++; });
  await sleep(140);
  const docB = domB.window.document;
  docB.getElementById('apiKey').value = 'test-key';
  micRms = 0.05; // a live floor is present
  const gumB0 = gumB;
  docB.getElementById('recordBtn').click(); // start -> reuse -> probe reads a floor -> NO rebuild
  await sleep(120);
  check('s23p: a healthy phone mic does NOT trigger a probe rebuild', gumB === gumB0, 'gum delta ' + (gumB - gumB0));
  check('s23p: a healthy press starts recording promptly', domB.window._recStarts === 1, 'recStarts ' + domB.window._recStarts);
  docB.getElementById('recordBtn').click(); // clean up the take
  await sleep(200);
  micRms = 0.05;
}

// ===== Scenario 37: FRESH-graph silent capture (the VPIO wall) =====
// The reused-only probe missed this: after an iOS interruption even a FRESH
// getUserMedia stream can deliver pure silence while looking live — and on the
// phone surface most presses ARE fresh graphs (the between-takes corpse guard
// sets audioSuspect). The press must probe the fresh graph too and fail LOUD
// pre-capture when it is silent.
console.log('--- scenario 37: fresh-graph silent capture fails loud BEFORE capture ---');
{
  let gum37 = 0;
  // No micGranted seed: the graph is NOT warmed at boot, so the press builds a
  // FRESH graph (gum #1), probes it, rebuilds once (gum #2), then fails loud.
  const dom37 = mkProbeDom(() => { gum37++; }, { bigButtonMode: 'always' });
  await sleep(140);
  const doc37 = dom37.window.document;
  doc37.getElementById('apiKey').value = 'test-key';
  micRms = 0.0; // the fresh stream is silent (VPIO wall)
  doc37.getElementById('recordBtn').click();
  await sleep(1400); // fresh build + two settle windows
  check('s37: a silent FRESH graph gets one rebuild then fails pre-capture', gum37 === 2, 'gum ' + gum37);
  check('s37: the fresh-graph silent press is loud (MIC NOT CAPTURING)', doc37.getElementById('status').textContent.includes('MIC NOT CAPTURING'), doc37.getElementById('status').textContent);
  check('s37: REC never started on the silent fresh graph', dom37.window._recStarts === 0, 'recStarts ' + dom37.window._recStarts);
  check('s37: sentinel copied for the silent fresh press', dom37.window._clip === '##DICTATION_FAILED##', JSON.stringify(dom37.window._clip));
  micRms = 0.05;
}

// ===== Scenario 38: idle mic-health sampler (big-button surface) =====
// Between takes iOS can kill the retained mic with NO event. The idle sampler
// reads one analyser frame every MIC_IDLE_PROBE_MS (4 s): two consecutive dead
// frames -> proactive rebuild while idle (free) + the "Mic ⚠ rebuilding" pill;
// a live mic shows "Mic ✓". It never runs mid-recording (sessions own the mic;
// the watchdog covers them).
console.log('--- scenario 38: idle mic-health sampler (proactive corpse rebuild) ---');
{
  let gum38 = 0;
  const dom38 = mkProbeDom(() => { gum38++; });
  await sleep(140); // boot warm (gum #1)
  const doc38 = dom38.window.document;
  doc38.getElementById('apiKey').value = 'test-key';
  micRms = 0.05;
  doc38.getElementById('recordBtn').click(); // healthy press -> recording
  await sleep(150);
  check('s38: recording started for the mid-take leg', dom38.window._recStarts === 1, 'recStarts ' + dom38.window._recStarts);
  micRms = 0.0; // the mic dies mid-take: the sampler must NOT rebuild during a session
  const gumMid = gum38;
  await sleep(4400); // one sampler tick while recording
  check('s38: the sampler never rebuilds mid-recording', gum38 === gumMid, 'gum delta ' + (gum38 - gumMid));
  doc38.getElementById('recordBtn').click(); // stop (empty take fails loud; not the point here)
  await sleep(400);
  const gumIdle = gum38;
  await sleep(8800); // two idle sampler ticks with a dead mic -> proactive rebuild
  check('s38: two dead idle frames trigger a proactive rebuild', gum38 === gumIdle + 1, 'gum delta ' + (gum38 - gumIdle));
  micRms = 0.05; // the rebuilt mic is live again
  await sleep(4400); // next idle tick sees the floor
  check('s38: the pill reads Mic ✓ once a live floor is back', doc38.getElementById('bigMicPill').textContent.includes('Mic ✓'), doc38.getElementById('bigMicPill').textContent);
  micRms = 0.05;
}

// ===== Scenario 34: manual "Send to desktop" + the queue chip =====
// The universal recovery for a stranded note: on a joined device, the box, the
// history rows, and the expanded big-peek can push their text to the desktop
// through the normal queue+flush+cue path (fresh delivery_id, cleaned text,
// idle-only). The queue chip makes undelivered notes visible and tap-flushable.
console.log('--- scenario 34: manual send-to-desktop + queue chip ---');
{
  const fetch34 = [];
  let deliver34 = 'ok'; // 'ok' | 'fail'
  const dom34 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock'; win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.mediaDevices = { getUserMedia: () => { micTrack.readyState = 'live'; micTrack.muted = false; return Promise.resolve(mockStream); }, addEventListener: () => {} };
      win.fetch = (url, opts) => {
        fetch34.push({ url: String(url), opts: opts || {} });
        if (String(url).includes('/deliver')) {
          if (deliver34 === 'fail') return Promise.resolve({ ok: false, status: 500, text: () => Promise.resolve('boom') });
          return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
        }
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"Fresh take."}') });
      };
      win.MediaRecorder = class { constructor() { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); } };
      const Sock = class extends MockWS {}; Sock.CONNECTING = 0; Sock.OPEN = 1; Sock.CLOSING = 2; Sock.CLOSED = 3; win.WebSocket = Sock;
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ joinedSessionCode: 'SEND01', micGranted: true }));
      win.localStorage.setItem('scribe_v2_transcripts_v9', JSON.stringify([
        { text: 'History note from earlier. ', createdAt: new Date().toISOString(), engine: 'batch' },
      ]));
    },
  });
  await sleep(180);
  const doc34 = dom34.window.document;
  const st34 = () => doc34.getElementById('status').textContent;
  const deliveries34 = () => fetch34.filter((c) => c.url.includes('/deliver') && c.opts && String(c.opts.body).includes('phone_delivery'));
  doc34.getElementById('apiKey').value = 'test-key';

  // (a) The box's send button: visible when joined, sends the restored note.
  const sendBtn = doc34.getElementById('sendDesktopBtn');
  check('s34a: the send button is visible on a joined device', sendBtn.style.display !== 'none' && !sendBtn.disabled, sendBtn.style.display + '/' + sendBtn.disabled);
  const n0 = deliveries34().length;
  sendBtn.click();
  await sleep(250);
  const sent = deliveries34().slice(n0);
  check('s34a: the send POSTs a fresh phone_delivery to the joined code', sent.length === 1 && sent[0].url.includes('/api/session/SEND01/deliver'), JSON.stringify(sent.map((c) => c.url)));
  check('s34a: the sent text is the cleaned box note', sent.length && JSON.parse(sent[0].opts.body).text.includes('History note from earlier.'), sent.length && sent[0].opts.body);
  check('s34a: a fresh delivery_id rides the manual send', sent.length && !!JSON.parse(sent[0].opts.body).delivery_id, sent.length && sent[0].opts.body);
  check('s34a: the delivered manual send announces Done', st34().includes('Delivered to the desktop clipboard. Done!'), st34());

  // (b) Mid-session the manual send is a no-op (idle-only guard).
  micRms = 0.05;
  doc34.getElementById('recordBtn').click();
  await sleep(120);
  const n1 = deliveries34().length;
  check('s34b: the send button is disabled mid-session', sendBtn.disabled === true, String(sendBtn.disabled));
  sendBtn.click();
  await sleep(120);
  check('s34b: a mid-session send click POSTs nothing', deliveries34().length === n1, deliveries34().length - n1 + ' extra');
  doc34.getElementById('recordBtn').click(); // stop; the take relays normally (+1 delivery)
  await sleep(500);

  // (c) History rows carry a per-row send button while joined.
  doc34.getElementById('toggleHistoryBtn').click();
  await sleep(30);
  const rowSend = Array.from(doc34.querySelectorAll('#history button')).find((b) => b.textContent.includes('Send to desktop'));
  check('s34c: history rows offer Send to desktop while joined', !!rowSend, Array.from(doc34.querySelectorAll('#history button')).map((b) => b.textContent).join('|'));
  const n2 = deliveries34().length;
  rowSend.click();
  await sleep(250);
  check('s34c: the history-row send delivers that row\'s text', deliveries34().length === n2 + 1, (deliveries34().length - n2) + ' posts');

  // (d) The expanded big-peek offers the send button on the phone surface.
  doc34.getElementById('bigPeekBar').click(); // expand
  await sleep(30);
  const bigSend = doc34.getElementById('bigSendBtn');
  check('s34d: the expanded big-peek shows Send to desktop again', bigSend.style.display !== 'none', bigSend.style.display);

  // (e) A failed delivery queues + the chip shows; a tap retries and clears it.
  deliver34 = 'fail';
  doc34.getElementById('recordBtn').click();
  await sleep(120);
  doc34.getElementById('recordBtn').click();
  await sleep(600);
  const chip = doc34.getElementById('bigQueueChip');
  check('s34e: an undelivered note lights the queue chip', chip.style.display !== 'none' && chip.textContent.includes('waiting to send'), chip.textContent);
  deliver34 = 'ok';
  chip.click();
  await sleep(300);
  check('s34e: tapping the chip flushes the queue', chip.style.display === 'none', chip.style.display + ' / ' + chip.textContent);

  // (f) Unjoined devices never show the send affordances.
  doc34.getElementById('phoneLeaveBtn').click();
  await sleep(30);
  check('s34f: the send button hides when not joined', sendBtn.style.display === 'none', sendBtn.style.display);
}

// ===== Scenario 25w: joined + degraded outcome — the relay ack must WARN, never doneBeep =====
// A large speaker-filter cut is a degraded outcome. On a joined phone the relay
// ack owns the outcome cue — and before this fix a "delivered" ack played the
// clean doneBeep over degraded content. Now: delivered + degraded ⇒ VERIFY
// status + warnBeep.
console.log('--- scenario 25w: joined degraded outcome gets warnBeep on the delivered ack ---');
{
  const vibes25w = [];
  const dom25w = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock'; win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.navigator.vibrate = (p) => { vibes25w.push(p); return true; };
      win.navigator.mediaDevices = { getUserMedia: () => { micTrack.readyState = 'live'; micTrack.muted = false; return Promise.resolve(mockStream); }, addEventListener: () => {} };
      win.MediaRecorder = class {
        constructor() { this.state = 'inactive'; }
        static isTypeSupported() { return false; }
        start() { this.state = 'recording'; }
        stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); }
      };
      const Sock = class extends MockWS {}; Sock.CONNECTING = 0; Sock.OPEN = 1; Sock.CLOSING = 2; Sock.CLOSED = 3; win.WebSocket = Sock;
      // Two speakers, 3 of 9 words from a bystander: a LARGE (>=15%) removal ⇒ degraded.
      const words = ['Assessment', 'and', 'plan', 'continue', 'current', 'medications'].map((t) => ({ text: t, type: 'word', speaker_id: 'speaker_0' }))
        .concat(['did', 'you', 'eat'].map((t) => ({ text: t, type: 'word', speaker_id: 'speaker_1' })));
      win.fetch = (url) => {
        if (String(url).includes('/deliver')) {
          return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"ok":true,"listeners":1}') });
        }
        return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve(JSON.stringify({ text: 'Assessment and plan continue current medications did you eat', words: words })) });
      };
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ joinedSessionCode: 'JOINW1', micGranted: true }));
    },
  });
  await sleep(180); // boot: persisted join restores -> big-button surface (diarize applies)
  const doc25w = dom25w.window.document;
  doc25w.getElementById('apiKey').value = 'test-key';
  micRms = 0.05;
  doc25w.getElementById('recordBtn').click();
  await sleep(140);
  doc25w.getElementById('recordBtn').click();
  await sleep(450); // upload -> degraded filter -> relay ack (listeners:1)
  const st25w = doc25w.getElementById('status');
  check('s25w: delivered ack on a degraded outcome says VERIFY (not Done!)',
    st25w.textContent.includes('VERIFY') && !st25w.textContent.includes('Done!'), st25w.textContent);
  check('s25w: degraded relay status is warn class', st25w.className.includes('warn'), st25w.className);
  check('s25w: the outcome haptic is the WARN pattern, not done',
    JSON.stringify(vibes25w[vibes25w.length - 1]) === JSON.stringify([90, 90, 90]), JSON.stringify(vibes25w));
  check('s25w: the filtered primary text still reached the local clipboard',
    (dom25w.window._clip || '').includes('Assessment and plan continue current medications') && !(dom25w.window._clip || '').includes('did you eat'),
    JSON.stringify(dom25w.window._clip));
}

// ===== Scenario 39: transcript-coverage guard =====
// The reported failure: a multi-minute dictation came back half-transcribed with
// a clean "Done!". The guard compares the transcript's last word end-time (and
// the service's decoded duration) against the speech the gate observed; a big
// shortfall is a degraded WARN — text still delivered — with a diag ring entry.
console.log('--- scenario 39: transcript-coverage guard ---');
{
  const mk39Dom = (state) => new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock'; win.URL.revokeObjectURL = () => {};
      // Skewable clock: the page's Date.now reads real time + skewMs, so a
      // "5-minute take" runs in milliseconds of wall time. The AudioContext
      // clock follows the same skewed time so the gate's hold/close advances.
      const realNow = Date.now.bind(Date);
      win.Date.now = () => realNow() + state.skewMs;
      const t0 = realNow();
      win.AudioContext = class extends MockAudioCtx {
        constructor() {
          super();
          Object.defineProperty(this, 'currentTime', { get: () => (realNow() + state.skewMs - t0) / 1000, configurable: true });
        }
      };
      win.navigator.mediaDevices = { getUserMedia: () => { micTrack.readyState = 'live'; micTrack.muted = false; return Promise.resolve(mockStream); }, addEventListener: () => {} };
      win.MediaRecorder = class {
        constructor() { this.state = 'inactive'; }
        static isTypeSupported() { return false; }
        start() { this.state = 'recording'; }
        stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); }
      };
      const Sock = class extends MockWS {}; Sock.CONNECTING = 0; Sock.OPEN = 1; Sock.CLOSING = 2; Sock.CLOSED = 3; win.WebSocket = Sock;
      win.fetch = (url, fOpts) => new Promise((resolve, reject) => {
        const t = setTimeout(() => resolve({ ok: true, status: 200, text: () => Promise.resolve(JSON.stringify(state.body)) }), state.delayMs || 5);
        if (fOpts && fOpts.signal) fOpts.signal.addEventListener('abort', () => { clearTimeout(t); const e = new Error('aborted'); e.name = 'AbortError'; reject(e); });
      });
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ micGranted: true }));
    },
  });

  // One simulated take: speak (gate open), optionally go silent, then jump the
  // clock ~290 s so the take reads as multi-minute, tick once more, and stop.
  const run39 = async (body, opts) => {
    const state = { skewMs: 0, body: body, delayMs: (opts && opts.delayMs) || 0 };
    const dom = mk39Dom(state);
    await sleep(160); // boot (micGranted warms the graph)
    const doc39 = dom.window.document;
    doc39.getElementById('apiKey').value = 'test-key';
    micRms = 0.05;
    doc39.getElementById('recordBtn').click();
    await sleep(150); // gate opens; speech observed
    if (opts && opts.silenceTail) { micRms = 0.0001; await sleep(100); } // true silence (below FLATLINE_RMS)
    state.skewMs = 290000;
    await sleep(150); // ticks under the skewed clock (speech continues, or the gate closes)
    doc39.getElementById('recordBtn').click(); // stop -> upload
    await sleep(400 + ((opts && opts.delayMs) || 0));
    micRms = 0.05;
    return dom.window;
  };
  const w39 = (win, sel) => win.document.getElementById(sel);
  const wordsUntil = (endSec) => [
    { text: 'First', type: 'word', start: 0.5, end: 1.2 },
    { text: 'half', type: 'word', start: 1.3, end: 2.0 },
    { text: 'only', type: 'word', start: endSec - 1, end: endSec },
  ];

  // (a) transcript stops at ~60 s of a ~290 s spoken take -> degraded WARN,
  //     text delivered, coverage-shortfall in the diag ring.
  const winA = await run39({ text: 'First half only.', words: wordsUntil(60), audio_duration_secs: 290 });
  const stA = w39(winA, 'status');
  check('s39a: a half-covered transcript is a WARN, never Done!', stA.className.includes('warn') && !stA.textContent.includes('Done!'), stA.textContent + ' / ' + stA.className);
  check('s39a: the warn says INCOMPLETE + VERIFY', stA.textContent.includes('INCOMPLETE') && stA.textContent.includes('VERIFY'), stA.textContent);
  check('s39a: the (possibly partial) text is still delivered', (winA._clip || '').includes('First half only.'), JSON.stringify(winA._clip));
  const ring39 = JSON.parse(winA.localStorage.getItem('scribe_v2_micfail_v9') || '[]');
  check('s39a: coverage-shortfall recorded in the diag ring', ring39.length && ring39[0].reason === 'coverage-shortfall', JSON.stringify(ring39[0] || null));
  check('s39a: the diag carries the coverage numbers', ring39.length && ring39[0].diag.includes('lastWordEnd:60s') && ring39[0].diag.includes('deadBand:'), ring39.length ? ring39[0].diag : 'no entry');

  // (b) full coverage (last word ends near the take end) -> clean Done!.
  const winB = await run39({ text: 'Full note to the end.', words: wordsUntil(285), audio_duration_secs: 290 });
  const stB = w39(winB, 'status');
  check('s39b: full coverage stays a clean Done!', stB.textContent.includes('Done!') && stB.className.includes('ok'), stB.textContent + ' / ' + stB.className);

  // (c) words without timestamps + no duration (older shape / API drift) ->
  //     the guard no-ops; clean Done!.
  const winC = await run39({ text: 'No timestamps here.', words: [{ text: 'No', type: 'word' }, { text: 'timestamps', type: 'word' }] });
  const stC = w39(winC, 'status');
  check('s39c: missing timestamps/duration is a clean no-op', stC.textContent.includes('Done!') && stC.className.includes('ok'), stC.textContent);

  // (d) no words[] but the service decoded far less audio than recorded ->
  //     degraded WARN via the duration check.
  const winD = await run39({ text: 'Decoded a fraction.', audio_duration_secs: 120 });
  const stD = w39(winD, 'status');
  check('s39d: a short decoded duration is a WARN', stD.className.includes('warn') && stD.textContent.includes('INCOMPLETE'), stD.textContent + ' / ' + stD.className);

  // (e) the gate-open baseline: speech only in the first moments, then TRUE
  //     silence while the button stays held ~290 s -> the transcript "ends
  //     early" against wall-clock but NOT against observed speech -> clean.
  const winE = await run39({ text: 'Short remark.', words: [{ text: 'Short', type: 'word', start: 0.1, end: 0.4 }, { text: 'remark', type: 'word', start: 0.5, end: 0.9 }], audio_duration_secs: 290 }, { silenceTail: true });
  const stE = w39(winE, 'status');
  check('s39e: held-but-silent tail does NOT false-warn (gate baseline)', stE.textContent.includes('Done!') && stE.className.includes('ok'), stE.textContent + ' / ' + stE.className);
}

// ===== Scenario 40: duration-aware upload deadline =====
// A ~290 s take extends the transcription deadline to ~75 s; a response that
// arrives after 16 s (past the old flat 15 s abort) must still succeed.
console.log('--- scenario 40: duration-aware upload deadline (16 s response on a long take succeeds) ---');
{
  const state40 = { skewMs: 0, body: { text: 'Long take transcribed late but fine.', words: [{ text: 'fine', type: 'word', start: 288, end: 289 }], audio_duration_secs: 290 }, delayMs: 16000 };
  const dom40 = new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock'; win.URL.revokeObjectURL = () => {};
      const realNow = Date.now.bind(Date);
      win.Date.now = () => realNow() + state40.skewMs;
      const t0 = realNow();
      win.AudioContext = class extends MockAudioCtx {
        constructor() {
          super();
          Object.defineProperty(this, 'currentTime', { get: () => (realNow() + state40.skewMs - t0) / 1000, configurable: true });
        }
      };
      win.navigator.mediaDevices = { getUserMedia: () => { micTrack.readyState = 'live'; micTrack.muted = false; return Promise.resolve(mockStream); }, addEventListener: () => {} };
      win.MediaRecorder = class {
        constructor() { this.state = 'inactive'; }
        static isTypeSupported() { return false; }
        start() { this.state = 'recording'; }
        stop() { if (this.state === 'inactive') return; this.state = 'inactive'; if (this.ondataavailable) this.ondataavailable({ data: new win.Blob([new Uint8Array(2048)], { type: 'audio/webm' }) }); if (this.onstop) this.onstop(); }
      };
      const Sock = class extends MockWS {}; Sock.CONNECTING = 0; Sock.OPEN = 1; Sock.CLOSING = 2; Sock.CLOSED = 3; win.WebSocket = Sock;
      win.fetch = (url, fOpts) => new Promise((resolve, reject) => {
        const t = setTimeout(() => resolve({ ok: true, status: 200, text: () => Promise.resolve(JSON.stringify(state40.body)) }), state40.delayMs);
        if (fOpts && fOpts.signal) fOpts.signal.addEventListener('abort', () => { clearTimeout(t); const e = new Error('aborted'); e.name = 'AbortError'; reject(e); });
      });
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({ micGranted: true }));
    },
  });
  await sleep(160);
  const doc40 = dom40.window.document;
  doc40.getElementById('apiKey').value = 'test-key';
  micRms = 0.05;
  doc40.getElementById('recordBtn').click();
  await sleep(150);
  state40.skewMs = 290000; // the take "lasted" ~290 s -> deadline 15 s + min(60 s, 25%) = 75 s
  await sleep(150);
  doc40.getElementById('recordBtn').click(); // stop -> upload; response lands after 16 s REAL time
  await sleep(17000);
  const st40 = doc40.getElementById('status');
  check('s40: a 16 s transcription on a long take succeeds (old flat 15 s would abort)',
    st40.textContent.includes('Done!') && st40.className.includes('ok'), st40.textContent + ' / ' + st40.className);
  check('s40: the late text reached the clipboard', (dom40.window._clip || '').includes('Long take transcribed late'), JSON.stringify(dom40.window._clip));
}

// ===== Scenario 41: journal cap honesty =====
// A crash-recovery capped at JOURNAL_MAX_CHUNKS is REAL but PARTIAL — the
// banner must say only the first ~N minutes were saved. An uncapped one must not.
console.log('--- scenario 41: journal cap honesty ---');
{
  const JOURNAL_CAP = 1800; // mirrors JOURNAL_MAX_CHUNKS in worker.js
  const seedJournal = (idb, chunkCount) => new Promise((resolve, reject) => {
    const req = idb.open('scribe_v2_journal', 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      db.createObjectStore('sessions', { keyPath: 'id' });
      const cs = db.createObjectStore('chunks', { keyPath: 'k', autoIncrement: true });
      cs.createIndex('sid', 'sid', { unique: false });
    };
    req.onsuccess = () => {
      const db = req.result;
      const tx = db.transaction(['sessions', 'chunks'], 'readwrite');
      tx.objectStore('sessions').put({ id: 'jSEED', createdAt: new Date().toISOString(), mimeType: 'audio/webm', joined: false, base: '', state: 'recording' });
      for (let i = 0; i < chunkCount; i++) tx.objectStore('chunks').put({ sid: 'jSEED', buf: new ArrayBuffer(1024) });
      tx.oncomplete = () => { db.close(); resolve(); };
      tx.onerror = () => reject(tx.error);
    };
    req.onerror = () => reject(req.error);
  });
  const mk41Dom = (idb) => new JSDOM(html, {
    runScripts: 'dangerously', url: 'https://dictation.test/',
    beforeParse(win) {
      win.isSecureContext = true;
      Object.defineProperty(win.document, 'visibilityState', { value: 'visible', configurable: true });
      win.navigator.clipboard = { writeText: (t) => { win._clip = t; return Promise.resolve(); } };
      win.URL.createObjectURL = () => 'blob:mock'; win.URL.revokeObjectURL = () => {};
      win.AudioContext = MockAudioCtx;
      win.indexedDB = idb;
      win.navigator.mediaDevices = { getUserMedia: () => { micTrack.readyState = 'live'; micTrack.muted = false; return Promise.resolve(mockStream); }, addEventListener: () => {} };
      win.MediaRecorder = class { constructor() { this.state = 'inactive'; } static isTypeSupported() { return false; } start() { this.state = 'recording'; } stop() { this.state = 'inactive'; if (this.onstop) this.onstop(); } };
      const Sock = class extends MockWS {}; Sock.CONNECTING = 0; Sock.OPEN = 1; Sock.CLOSING = 2; Sock.CLOSED = 3; win.WebSocket = Sock;
      win.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve('{"text":"recovered"}') });
      win.localStorage.setItem('scribe_v2_settings_v9', JSON.stringify({}));
    },
  });

  // (a) a capped orphan (exactly the cap) -> the banner admits the partial save.
  const idbCap = new IDBFactory();
  await seedJournal(idbCap, JOURNAL_CAP);
  const domCap = mk41Dom(idbCap);
  await sleep(400); // boot -> restoreJournal
  const msgCap = domCap.window.document.getElementById('journalRecoverMsg').textContent;
  const shownCap = domCap.window.document.getElementById('journalRecover').style.display !== 'none';
  check('s41a: a capped recovery banner shows', shownCap, 'display=' + domCap.window.document.getElementById('journalRecover').style.display);
  check('s41a: a capped recovery says only the first ~30 minutes were saved', msgCap.includes('first 30 minutes') && msgCap.includes('incomplete'), msgCap);

  // (b) an uncapped orphan -> no partial-save note.
  const idbOk = new IDBFactory();
  await seedJournal(idbOk, 3);
  const domOk = mk41Dom(idbOk);
  await sleep(400);
  const msgOk = domOk.window.document.getElementById('journalRecoverMsg').textContent;
  const shownOk = domOk.window.document.getElementById('journalRecover').style.display !== 'none';
  check('s41b: an uncapped recovery banner shows', shownOk, 'display=' + domOk.window.document.getElementById('journalRecover').style.display);
  check('s41b: an uncapped recovery has no partial-save note', !msgOk.includes('first 30 minutes'), msgOk);
}

console.log(failures === 0 ? 'ALL SCENARIOS PASSED' : failures + ' FAILURES');
process.exit(failures ? 1 : 0);
