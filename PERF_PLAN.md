# PERF_PLAN.md — batch latency + mic accuracy + the minimized-window cue

Working plan for the three open complaints against the batch-only product:

1. **Accuracy still suffers on certain mics.**
2. **Release→clipboard latency is worse than it should be**, for short utterances (the
   dominant case) *and* for a longer dictated paragraph (the general-user case).
3. **In a minimized PWA window on a machine with no speakers, you cannot tell you are
   recording.**

Realtime is **out of scope** — it stays archived in `REALTIME_HANDOFF.md`. This plan
supersedes the archived `LATENCY_PLAN.md`, which was written for the hybrid finalize path
that no longer exists.

Everything here is subordinate to the prime directive: **failures must be loud, and a
dictation must never be lost.** Any step that widens a loss window is rejected, and the
hard invariants in `CLAUDE.md` (F13/F14, the sentinel, one delivery per session, the
load-bearing gate, loud finalize, coverage guard) are constraints, not negotiables.

---

## 0. Where the time actually goes

Post-release budget for a ~5 s utterance, from the current code path:

| Stage | Code | Est. | Attackable |
|---|---|---|---|
| Recorder flush + Blob assembly | `stopRecording` → `onstop` → `finalizeSession` | 20–60 ms | marginal |
| Upload transfer (~40 KB webm, warm TLS) | `batchTranscribe` fetch | 30–100 ms | yes |
| Worker buffer + re-serialize + hop to ElevenLabs | `handleTranscribeBatch` | 50–150 ms | partly |
| **ElevenLabs decode + inference** | service | **800–2000 ms** | only via request shape |
| Response parse, splice, clipboard, beep | `finishBatchSession` → `deliverFinalText` | 20–60 ms | marginal |
| AHK `ClipWait` + rewrite + second `ClipWait` | `hotkey.ahk` | 30–150 ms | yes |
| **You noticing, alt-tabbing to Cerner, pasting** | human | **1000–3000 ms** | **yes — biggest term** |

Two conclusions drive the sequencing:

- **The service term is the floor and it is not directly controllable.** The only lever on
  it is the *shape* of the request (container/codec, and possibly keyterm volume).
- **The largest single term is the human tail**, and it is entirely ours: today, after a
  dictation, focus is sitting on the dictation window (AHK activated it and never gave it
  back), so every take ends with an alt-tab. Fixing that is ~20 lines of AutoHotkey and
  beats every microsecond of transport work.

Every number above is an estimate. **Phase 0 exists to replace them with measurements
before any speculative work is done**, because the Phase 3/4 decisions invert depending on
the real split between transfer and inference.

## 0b. Accuracy hypotheses, ranked

| # | Hypothesis | Confidence | Fix |
|---|---|---|---|
| A | **Wrong input device.** A work laptop with a dock/headset routinely defaults to a Bluetooth HFP or webcam mic. `micDiag` already reports `micSR`, but only on failure, and nothing lets you pin the good mic. | high | Phase 2a |
| B | **The gate clips word onsets.** Detection lag = 30 ms tick + ~21 ms analysis window + 20 ms attack ramp, gated on RMS > 0.03. Utterance-initial unvoiced consonants (f, s, h, p, t, th) sit under that, so the first 50–100 ms of "fifteen"/"sepsis"/"Thursday" is attenuated **in the audio that gets uploaded**. Worse on quiet mics, which is exactly the reported symptom. | high | Phase 1c |
| C | **The gate is the wrong tool at all in batch.** It exists to keep silence out of the upload, but Scribe v2 handles silence and pauses fine, and the gate is what produces the dead-band ("VERY LOW MIC LEVEL") failures. On an odd mic, uploading the ungated signal may simply be better. | medium | Phase 2b |
| D | **Release clips the last syllable.** `mediaRecorder.stop()` fires on F14 with no tail. | medium | Phase 1c |
| E | **Narrowband capture.** A 8/16 kHz HFP mic is an accuracy killer no request shape can fix. | medium | Phase 2a |
| F | **Keyterm volume.** `always` (29) + ER (69) = 98; + wound (146) = 175. Over 100 keyterms ElevenLabs applies a **20-second minimum billable duration** — a real cost issue on 3-second utterances, and an unmeasured latency question. | low (latency) / high (cost) | Phase 0 A/B |

Note B/C/D are all *client-side audio* problems, not model problems. That matches the
report: Scribe v2 is accurate, but it is being fed clipped, gated, sometimes narrowband
audio.

---

## Phase 0 — Instrument before optimizing

**Goal:** replace the table in §0 with per-take measured numbers from the actual work
machine and network, exportable.

**Nothing in Phases 2–4 should be built before this has run for a few real shifts.**

### 0.1 Client stage stamps

Add a per-take `takeTimings` object, reset in `startRecording` alongside the other
per-session resets (`worker.js` ~3322). Stamp `performance.now()` at:

| Field | Where |
|---|---|
| `release` | `stopRecording()` entry (`worker.js:3428`) |
| `onstop` | `finalizeSession()` entry (`worker.js:3449`) |
| `uploadStart` | `batchTranscribe`, immediately before `fetch` (`worker.js:2682`) |
| `headers` | after the `await fetch(...)` resolves |
| `body` | after `await res.text()` |
| `delivered` | `deliverFinalText`, after the clipboard write |

Derived and stored: `flush`, `upload+service` (`headers-uploadStart`), `download`,
`deliver`, `total`. Recorded alongside: `recMs`, blob bytes, chunk count, keyterm count,
`diarize` on/off, `file_format`, and the `micSR`/device label already in `micDiag`.

### 0.2 Server stage stamps

In `handleTranscribeBatch` (`worker.js:390`), stamp around the `formData()` parse and
around the ElevenLabs `fetch`, and emit:

```
Server-Timing: parse;dur=<ms>, el;dur=<ms>
```

Same-origin, so the client reads it with `res.headers.get("server-timing")` — no
`Access-Control-Expose-Headers` needed. This is the number that splits "our transport" from
"their inference", and it is the single most decision-relevant measurement in this plan.

> **Constraint:** durations only. `CLAUDE.md` forbids logging request URLs in the Worker
> (the phone-link WS upgrade carries the passphrase as `?auth=`), and the batch POST body
> carries the passphrase — **never log the body or the URL**, and never put either in a
> header.

### 0.3 Surface

- New bounded ring `scribe_v2_timing_v9` (own key, additive, per-device, never wiped by a
  settings change — same precedent as `scribe_v2_micfail_v9`), cap ~50 entries.
- A compact readout in **Advanced**: `Last take: flush 42 · upload+service 980 · deliver 24
  = 1046 ms (5.2 s take, 41 KB, webm)`.
- A **Copy timing log** button (TSV) so a shift's worth can be pasted into an analysis.
- `?perf=1` also `console.table`s each take.

Status-line output stays untouched — the clinical status line must not gain debug text.

### 0.4 The A/Bs Phase 0 must answer

Run each ≥10 short takes and ≥3 long takes, same mic, same network:

1. Keyterms **on vs off** (does the ~20 % surcharge / >100-term minimum move latency?).
2. `diarize` on vs off (desktop already forces off; confirm the phone cost).
3. Short (≤10 s) vs long (≥120 s) — does service time scale with duration or is there a
   fixed floor?

**Done when:** you can state, from data, what fraction of release→clipboard is ElevenLabs
inference. If it is >70 %, Phases 3 and 4 are mostly futile and the plan ends at Phase 2.

**Risk:** none. Purely additive instrumentation, no path changes.

---

## Phase 1 — The certain wins (indicator + human tail + word boundaries)

Independent of the STT contract; ships as one PR with Phase 0.

### 1a. Recording cue that survives a minimized window

Three surfaces, most reliable first. **No new state** — all three derive from the state
`updateBigScreen` (`worker.js:4977`) already computes.

Refactor: extract that derivation into `currentUiState()` returning
`alarm|rec|connecting|busy|fail|warn|ok|idle`. `updateBigScreen` consumes it (unchanged
behavior) and a new `applyWindowState()` consumes it too. Because `setStatus` (1785),
`setMicPill` (1792) and `setLinkPill` (1802) all already call `updateBigScreen()`, hooking
in there catches every transition for free.

**(i) Whole-window state wash — the load-bearing one.**
`applyWindowState()` sets `document.body.dataset.state`. CSS paints the *entire* window:

- `rec` → saturated red background + pulsing accent border
- `busy` → amber
- `ok` → green, fading out after ~1.2 s
- `fail` / `alarm` → hard red, persistent

If any sliver of the window is visible next to Cerner, a full-window red wash is
unmistakable at a glance — no text to read, no speaker needed. Gate the wash on
`body:not(.bigbtn)` so the phone overlay (which has its own `#bigUi[data-screen]` styling)
is untouched, and make its intensity a per-device setting (`windowTint`: `full` / `border`
/ `off`, default `full`).

**(ii) Title + favicon.** `document.title = "● REC · Dictation"` while recording, restored
on idle; swap `link[rel=icon]` to a red-dot data-URI. Both show in the taskbar and window
title even when the window is too small to show content.

**(iii) Badge + theme-color — speculative, feature-detected, never the only cue.**
`navigator.setAppBadge()` and a dynamic `<meta name="theme-color">` (which tints the
installed-PWA title bar on some Chrome/Edge versions). Both are "limited availability" per
MDN and neither is confirmed for a desktop-installed PWA on your work build — wrap in
try/catch, verify on the actual machine, and treat as a bonus on top of (i) and (ii).

**(iv) AHK on-screen bar** — see 1b, and this is the one that works even when the PWA
window is fully hidden.

### 1b. AutoHotkey: give focus back, and show state on screen

The biggest perceived-latency win in the whole plan, and it never touches the browser.

Current `*CapsLock::` (`hotkey.ahk:139`) activates `DICT_HWND`, dictates, waits for the
clipboard, and **leaves you on the dictation window**.

Changes:

1. **Capture the origin window.** `PREV_HWND := WinActive("A")` *before*
   `ActivateWindow(DICT_HWND)`.
2. **Return focus** after the clipboard lands and is cleaned:
   `if (RETURN_FOCUS && PREV_HWND && PREV_HWND != DICT_HWND) ActivateWindow(PREV_HWND)`.
   New config `RETURN_FOCUS := true`. This alone removes the alt-tab.
3. **Optional auto-paste.** New config `AUTO_PASTE := false` (**default off**). When on, and
   *only* when focus was restored to the exact `PREV_HWND` captured at press time, and the
   result was not the sentinel: `Send("^v")`. Off by default deliberately — an auto-paste
   into a drifted caret is a wrong-chart risk, and that decision is yours to opt into.
4. **Always-on-top state bar.** One `Gui` created at startup:
   `+AlwaysOnTop -Caption +ToolWindow`, and critically `WS_EX_NOACTIVATE (0x08000000)` so
   showing it can **never** steal focus from Cerner. States: `● HOLD` (red, while CapsLock
   is physically held) → `WORKING` (amber, while `ClipWait` runs) → `READY` (green, 800 ms)
   or `FAILED` (red, 2 s, on sentinel/timeout). Position configurable; default bottom-right.
   This is the indicator that works when the PWA window is completely covered.
5. **Skip the redundant clipboard rewrite.** The browser's `cleanTranscript` already strips
   newlines, collapses spaces and appends the trailing space. When `txt` already matches
   what AHK would produce, skip the second `A_Clipboard :=` and its `ClipWait(2,1)`.
6. **Lower `MIN_HOLD`.** 350 ms discards genuine short-form utterances ("yes", "normal",
   "negative"). Drop to ~200 ms. Note the browser runs a real session either way — F13/F14
   are sent regardless — so this only controls whether AHK pastes the result.

`CLIP_TIMEOUT := 90` still covers the deadline (unchanged by this plan).

### 1c. Gate look-ahead + release tail — recover the clipped first and last words

Targets hypotheses B and D. **These two are one change**, because a look-ahead delay in the
recording path means the tail is still in the delay line at `stop()`.

**Graph change** (`ensureAudio`, `worker.js:3032`):

```
now:  hpFilter → analyserNode                       (meter / watchdog / probe)
      hpFilter → gateNode → destNode                (recorded)

new:  hpFilter → analyserNode                       (UNCHANGED — undelayed)
      hpFilter → lookaheadNode → gateNode → destNode (recorded, delayed)
```

The gate decision is still computed from the **undelayed** analyser, but applied to audio
that is `GATE_LOOKAHEAD_MS` behind it — so by the time the gain has ramped up, the audio
flowing through is the audio from *before* the threshold crossing. That is precisely the
recovered onset.

**Why this breaks nothing:** `probeMicAlive`, the flatline watchdog, `maxRmsSeen`, the
meter, the capture waveform, and `lastGateOpenAtMs` all read `analyserNode`, which stays
pre-delay. The coverage guard's baseline is therefore unchanged; `recEndedAt` grows by the
tail, which is ~200 ms against a 20 s slack floor.

**Release tail** (`stopRecording`, `worker.js:3428`): defer `mediaRecorder.stop()` by
`GATE_LOOKAHEAD_MS + RELEASE_TAIL_MS` instead of calling it inline. `stopping = true` and
the "Stopping — preparing upload…" status are still set immediately, so there is no
perceived hang. The existing `stopping` guard already prevents a double-stop from a second
F14; `pendingStart`/`cancelQueuedStart` behave exactly as they do across the existing
finalize window.

**New per-device constants (both slider-exposed in Advanced, 0 = disabled):**

| Constant | Default | Meaning |
|---|---|---|
| `GATE_LOOKAHEAD_MS` | 120 | Delay in the recorded branch only, so the gate opens *before* the audio it gates |
| `RELEASE_TAIL_MS` | 80 | Extra capture past release, so the last syllable is not truncated |

**Honest trade:** this adds ~200 ms of machine latency to every take. It is worth it —
a clipped drug name costs an entire redictation plus a chart-accuracy risk, which is
several seconds and a safety issue, not 200 ms. But it is exposed and reversible, and
Phase 0's ring will show you the exact cost.

**Done when:** the timing ring shows the added ~200 ms, and dictating "fifteen milligrams
of famotidine" repeatedly on the problem mic no longer drops leading consonants.

---

## Phase 2 — Mic and capture-path accuracy

Highest-value phase for the stated accuracy complaint. Independent of Phase 3/4.

### 2a. Device picker + narrowband warning (hypotheses A, E)

- `navigator.mediaDevices.enumerateDevices()` → a `<select id="micDevice">` in Advanced
  listing `audioinput` devices (labels are available since permission is already granted).
- Persist `micDeviceId` (**per-device** setting), pass `deviceId: { exact: id }` into the
  `getUserMedia` constraints in `ensureAudio` (`worker.js:2937`).
- On `OverconstrainedError`, fall back to the default device **loudly** — a silent
  substitution is exactly the "certain mics" failure this is meant to fix.
- Repopulate on the `devicechange` listener that already exists.
- **Narrowband warning:** after `ensureAudio`, read `track.getSettings().sampleRate`; below
  24 kHz (or a label matching a Bluetooth hands-free pattern) raise a persistent warn pill:
  *"Mic is narrowband (16 kHz) — accuracy will suffer. Pick the headset's high-quality
  input, or the built-in mic."* The datum is already in `micDiag`; this promotes it from
  post-mortem to pre-flight.

### 2b. Record-path selector — gate bypass A/B (hypothesis C)

New **per-device** `recordPath`: `gated` (current, default) | `soft` | `raw`.

- `gated` — `hpFilter → lookahead → gateNode → destNode` (today).
- `soft` — same, but the gate's closed value is ~-20 dB instead of 0: attenuate background,
  never silence speech.
- `raw` — `hpFilter → lookahead → destNode`, bypassing `gateNode` entirely.

**The gate computation keeps running in every mode.** Only the *audio routing* changes. That
is what preserves every dependent diagnostic: `lastGateOpenAtMs` (coverage guard),
`gateOpenMs`/`deadBandMs`, `speechDetected`, the meter and the waveform all keep their
current meaning. `CLAUDE.md`'s "the post-gate recording is what gets uploaded" stays
structurally true — whatever is in the recorded branch is what is uploaded.

**One classification must be mode-aware:** the `lowLevel` empty-take branch in
`finishBatchSession` (`worker.js:3573`) tests `maxRmsSeen < gateOpen`. In `raw`/`soft` mode
quiet speech *does* record, so that branch would mislabel a genuine no-speech take. Suppress
it outside `gated` mode. Add `path:<mode>` to `micDiag` so every failure report says which
routing produced it.

### 2c. Dual-capture A/B tool — make accuracy measurable

To decide 2b with evidence instead of impression: an Advanced toggle (**off by default**)
that runs a *second* `MediaRecorder` on the raw branch during a take, plus a
**"Re-transcribe raw copy"** button beside the existing audio preview that uploads it and
shows both transcripts side by side.

Guards: manual and out-of-session only; never writes the clipboard, history, or the journal;
never touches `deliverFinalText`; off by default (it doubles capture memory). This converts
"accuracy is bad on certain mics" into a diffable pair of transcripts from *identical*
audio, which is the only way to settle B and C.

---

## Phase 3 — PCM upload for short takes (conditional on Phase 0)

The API reference is explicit: with `file_format=pcm_s16le_16` (16-bit PCM, 16 kHz, mono,
little-endian) *"latency will be lower than with passing an encoded waveform"*. Today every
take is webm/opus with `file_format=other` (`worker.js:2665`), so ElevenLabs decodes first.

**Build only if Phase 0 shows the service term is not overwhelmingly dominant.**

### Prior art — recover, don't rewrite

The removed realtime engine already had all of this. From `git show 73f7d1e^:worker.js`:
the `/pcm-pump.js` worklet module (~line 103–124, route at 210), `downsampleBuffer` (2190),
`floatTo16BitPCM` (2212).

### The trap, already paid for once

`REALTIME_HANDOFF.md` documents that loading the worklet from a **Blob URL** silently failed
to register the processor, so every session fell back to a main-thread `ScriptProcessor`
that starved under UI load and dropped frames — and the jsdom harness never caught it
because it *mocks* `addModule`/`AudioWorkletNode`.

Therefore:

- Serve the worklet as a real same-origin module at `GET /pcm-pump.js` and `addModule()`
  **that URL**.
- **If the worklet does not load, do NOT fall back to ScriptProcessor.** Fall back to the
  webm/MediaRecorder path. A degraded pump must never become the load-bearing capture path.
- Log which pump is active, and **verify in a real browser** — the harness cannot.

### Shape

- Tap the worklet off the **recorded** branch (post-gate, post-look-ahead) so PCM and webm
  capture bit-identical audio.
- Run PCM capture **alongside** `MediaRecorder`, never instead of it: MediaRecorder stays
  the crash journal source and the fallback.
- At stop: if `recMs <= PCM_MAX_TAKE_MS` (default 60 000) and the PCM buffer is intact,
  upload PCM with `file_format=pcm_s16le_16`; otherwise upload webm.
- **On any PCM-path failure, retry once with the in-memory webm blob before failing
  loudly.** The webm already exists, so this costs nothing and preserves "never lose a
  dictation".
- Worker: `file_format` is already passed through (`worker.js:420`); just permit
  `pcm_s16le_16`. The 1 KB floor and 25 MB cap are both fine (60 s PCM = 1.92 MB).

### The counter-pressure

16 kHz × 2 bytes = **32 KB/s**. A 5 s take is ~160 KB of PCM vs ~40 KB of webm — **4× the
upload bytes** on hospital wifi. Whether the decode saving beats the transfer cost is
exactly what Phase 0 measures. Hence the duration cap and the webm fallback.

---

## Phase 4 — Transport (measure, then probably don't)

### 4a. Worker pass-through

`handleTranscribeBatch` does `await request.formData()`, buffers the whole upload, rebuilds
a new `FormData`, and re-POSTs. A true pass-through (client builds the exact ElevenLabs
multipart body; credentials move to an `x-app-auth` header; Worker validates then pipes
`request.body`) removes one full buffer.

**The cost is real:** the Worker currently *enforces* `temperature=0`, `language_code=en`,
`num_speakers=1`, the diarize shape, and `sanitizeKeyterms` server-side. With a shared
master key, pass-through hands an authenticated client arbitrary parameters — including
billable ones (`entity_detection` is a 30 % surcharge). Expected saving: 50–150 ms, almost
certainly dominated by inference.

**Recommendation: don't**, unless Phase 0 shows the Worker hop is material. Keep server-side
enforcement.

### 4b. Streaming request body — analysed and rejected

Streaming the upload during the hold sounds ideal, but the constraints kill it:

- Chrome 105+ only, HTTPS only, **HTTP/2/3 only**, `duplex: 'half'` required. Safari and
  Firefox do not support it.
- **Intermediaries buffer.** Cloudflare sits in front of the Worker, and the Worker→
  ElevenLabs leg is a second hop.
- Decisively: **ElevenLabs batch STT is not a streaming API.** Inference cannot begin until
  the body is complete, so even a perfectly streamed upload saves only *transfer* time, not
  inference time.

For a 5 s / 40 KB utterance that is ~50 ms for a large amount of browser-specific
complexity. **Rejected for short form.** Revisit only if Phase 0 shows upload transfer
>300 ms on long takes, and then only for the long-take path.

---

## Explicitly rejected

| Idea | Why not |
|---|---|
| Scribe v2 Realtime / any realtime engine | Your call — too finicky. Door stays open via `REALTIME_HANDOFF.md`. |
| Split a take into parallel chunk uploads | Destroys cross-boundary context (accuracy) and breaks one-delivery-per-session. |
| `webhook=true` | Async delivery — strictly worse latency. |
| Client-side VAD trimming before upload | Same onset-clipping failure mode as the gate, with no service-side saving. |
| Drop `timestamps_granularity=word` to shrink the response | It is the coverage guard's input. Reliability outranks a few ms. |
| Bump the `_v9` suffix for new settings | Wipes every user's settings and history. All new fields are additive. |

---

## Settings inventory (portable vs per-device)

`CLAUDE.md` requires every new setting to declare its side of the future portability split.
**All new settings below are per-device** — they are mic-, machine-, and network-specific
tuning, so none of them complicate that split.

| Field | Phase | Side |
|---|---|---|
| `windowTint` (`full`/`border`/`off`) | 1a | per-device |
| `gateLookaheadMs` | 1c | per-device |
| `releaseTailMs` | 1c | per-device |
| `micDeviceId` | 2a | per-device |
| `recordPath` (`gated`/`soft`/`raw`) | 2b | per-device |
| `abCapture` | 2c | per-device |
| `pcmUpload` | 3 | per-device |

New localStorage key: `scribe_v2_timing_v9` (own ring, like `scribe_v2_micfail_v9`).

---

## Test plan

Following the existing numbering in `tests/flow.test.mjs` (next free numbers after 42):

| # | Scenario |
|---|---|
| **43** | Timing instrumentation: a take writes stage timings to the ring; `Server-Timing` is parsed; the ring is bounded; a *failed* take still records an entry. |
| **44** | Window state mirror: `body[data-state]` tracks rec→busy→ok/fail off the existing status/pill transitions; the title changes and is restored; the wash is never applied under `body.bigbtn`; `windowTint:"off"` disables it. |
| **45** | Look-ahead + release tail: `stop()` is deferred by the configured tail and the recorder is still stopped **exactly once**; the delay node is in the recorded branch only (analyser stays undelayed → watchdog, probe and `maxRmsSeen` unaffected); `0` disables it; a PTT queued during the tail still queues, and a release/F14 during it still cancels. |
| **46** | Device picker: a chosen `deviceId` rides into `getUserMedia`; `OverconstrainedError` falls back **loudly**; a narrowband `sampleRate` raises the warn. |
| **47** | Record path: `raw` bypasses `gateNode` in the recorded branch while the gate *computation* keeps running (`lastGateOpenAtMs`, watchdog, `maxRmsSeen` intact); the `lowLevel` classification is suppressed outside `gated`; `path:<mode>` appears in `micDiag`. |
| **48** | PCM upload: a short take posts `file_format=pcm_s16le_16`; a long take posts webm; a worklet load failure falls back to **webm, never ScriptProcessor**; a PCM upload failure retries once with the in-memory webm before failing loudly. |

**Harness notes:** the mock `AudioContext` needs `createDelay` (Phase 1c) and mocked
`audioWorklet.addModule`/`AudioWorkletNode` (Phase 3). Remember that mocking `addModule` is
exactly what hid the real worklet bug last time — **Phase 3 requires a manual browser check**
confirming the console names the active pump, in addition to scenario 48.

Standard gate for every PR in this plan:

```sh
node --check worker.js
# render through the real fetch handler and syntax-check the served inline script
npm install --no-save jsdom jsqr fake-indexeddb   # one command — --no-save prunes
node tests/flow.test.mjs
```

Pushing the working branch **deploys to the live worker** — validate before pushing.

---

## Sequencing

| Order | Work | Rationale |
|---|---|---|
| 1 | **Phase 0 + Phase 1 in one PR** | Independent of the STT contract. Delivers the recording cue and the human-tail fix immediately, and installs the measurement everything else depends on. |
| 2 | **Field-use gap (~a week)** | Read the timing ring and the Phase 0 A/Bs. This decides whether Phases 3–4 are worth anything. |
| 3 | **Phase 2** | Accuracy is the higher-stated priority, and 2a is the most likely single cause of the "certain mics" problem. |
| 4 | **Phase 3, only if the data says so** | Build only if decode is a material share of the service term. |
| 5 | **Phase 4 — expected to be dropped** | Documented above as analysed-and-rejected; revisit only on long-take transfer evidence. |

Keep `README.md`'s Roadmap updated in the same change as the code, per `CLAUDE.md`.
