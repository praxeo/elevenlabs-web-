# ElevenLabs Scribe v2 Dictation — working notes

Personal notes on this app: what it is, for debugging and development purposes. 

Goal is to create a web-only, dependency-free, highly accurate dictation tool that provides for short-form voice typing/dictation. The ideal use case is dictating clinical documentation, with domain specific prompting, focus on latency, and ability to work despite limitations of sandboxed/firewalled corporate environments. At this time, a cloudflare worker/DO serve at the edge and act as a go-between to ElevenLabs Scribe V2 API. 

Ideal Use: 
  -high quality microphone, installed to desktop as a PWA, and use autohotkey script to focus the window and activate dictation. Auto copy to clipboard, and then additional hotkey to paste into desired focused field. 

Phone dictation:
  -Using a QR or pairing code, link phone to same cloudflare worker, transmit audio to Elevenlabs via the worker, and then to the desktop where it can then enter the regular dictation workflow.
  -Primary drawback is IOS microphone permission/wedging issue that is seemingly insurmountable. This is an intrinsic limitation of PWAs in IOS. 
  -Workaround would be android or a device solely used for dictation that could be placed in focus mode.

Learnings so far:
  -Goal was a batch/realtime hybrid; where the realtime feedback would be backed up by a concurrent batch job that would be sent at the end of utterance. In practice, realtime transcription is not quite there. Attempted Soniox, ElevenLabs, Mistral. Soniox is close, but latency and accuracy issues were pervasive and      not fixable.
  -To me however, this represents the gold standard of dictation software. Seamless dictation, with high accuracy, minimal latency, all going to the cursor in real time, with the backup of the high domain-specific accuracy of a batch "recheck" at the end of utterance.
  -Realtime dictation products are close, but the multiple moving parts involved are a real last mile hurdle that is insurmountable at this time in this context.

## What it does

Hold a key, speak, release. The audio uploads to ElevenLabs Scribe v2 (batch) and the transcript lands on the clipboard. Batch-only, single speaker, medical config.

- **Push-to-talk.** Ctrl+Space (tap = start/stop, hold = talk-and-release). F13/F14 also start/stop, for the AutoHotkey relay (CapsLock → F13/F14).
- **The noise gate decides what's transcribed.** mic → high-pass → gate → recorder, and the post-gate recording is exactly what's uploaded — so gate tuning changes the transcript, not just a preview.
- **Live capture feedback.** A waveform, a "Hearing you" light, and a timer while recording, all on-device from the analyser. Nothing streams; it just proves the mic is live. (This is the reassurance realtime used to give, for free.)
- **One note per dictation.** ➕ Append next adds the next dictation onto the current note (a one-shot); append mode keeps chaining until I turn it off or clear the box. Always explicit — no hidden timer.
- **Copy files the note.** "Copy & clear" copies the note *and* readies a fresh box: the note drops into the **Last dictation** slot right below (with its own Copy + "➕ Append to this"). Starting a new dictation or "Clear dictation box" files it the same way. The box holds the note until then, so it stays editable/appendable until I'm done with it.
- **Everything is editable in place.** The active box, the Last dictation slot, and every History row are all directly hand-editable — click to place the caret and type. Edits persist on blur (history stamps "· edited"; Esc reverts a history row), and flow through to Copy, Append, and delivery.

### Audio cues — meant to be used without watching the screen

| Sound | Meaning |
|---|---|
| Single beep | Recording started |
| Rising double beep | Copied to the clipboard |
| Long low beep | Failure — sentinel `##DICTATION_FAILED##` copied, or the clipboard write failed. Don't paste. |
| Three descending beeps | Dead-mic alarm — recording but no signal (~2.5 s in) |
| Two beeps | Warn — a degraded but usable outcome; verify before pasting |

Start/done beeps are optional (Options). Failure and warn alarms always play, and reuse the running audio context so they sound even in a background tab.

## What's worked

- **Batch is the product.** Fast and accurate, dead simple, no streaming jitter. The three-engine era was a distraction (see below).
- **Loud failure, everywhere.** The prime directive — a silent failure is wrong text in a chart. Sentinel on the clipboard, alarms that always play, a dead-mic watchdog, and the last 100 transcripts kept in History so a botched copy is never a lost dictation.
- **The gate earns its keep.** It's the capture path *and* it drives the waveform feedback and the dead-mic watchdog — one analyser, three jobs.
- **Phone link durability.** The phone dictates and the text lands on the desktop clipboard. The delivery is queued on the phone (persisted, retried on reconnect and at boot), acked by listener count, replayed by the room, and deduped by id — so a dropped link, or a phone that dies after transcribing, recovers instead of losing the note.
- **Keeping the phone screen awake.** The real fix for "iOS keeps killing the mic between takes" was to *prevent* the interruption (hold the wake lock while the phone is on the big-button surface), not just recover after it.
- **Diarization keep-primary-speaker.** Drops a bystander voice the mic caught and reports how many words it removed. On by default, but only on the phone / big-button surface (where bystanders are the real risk) — a plain desktop keeps the proven single-speaker config. It only filters when one voice clearly dominates (so it never deletes my own words on an even split), a big cut warns and saves the unfiltered version to history, and it can never empty a clean note.

## What hasn't worked (and the lessons)

- **Realtime/hybrid streaming — removed.** A lot of work, and our live feed stayed jittery while the same engine was smooth on the vendor's own demo. Root cause: we captured raw PCM and downsampled/sent it on the *main thread*, so under render load it shipped jittery audio; batch (and the demo) use off-thread `MediaRecorder`. Batch was always flawless, so I cut realtime entirely. Full post-mortem and a resurrection path are in [`REALTIME_HANDOFF.md`](REALTIME_HANDOFF.md) — I haven't given up on it.
- **The hidden append time window.** Append used to depend on how many seconds had passed since the last dictation — the same action gave different results, and a pause past the cliff silently wiped a note. Deleted; append is now purely explicit.
- **Noise suppression / Voice Isolation don't stop other *voices*.** They strip *noise*; another person's speech is speech and survives them. The levers that actually work: hold the mic close (the biggest one), iOS Voice Isolation for noise, and the diarization filter for a second speaker. The one-time Mic tips card walks through them.
- **iOS hands back dead microphones.** After backgrounding, iOS leaves a dead mic track reporting `readyState:"live"`, unmuted, with no `ended` event — so the app trusted a corpse and recorded silence (the worst real bug: looked like it was recording, captured nothing). Fix: any backgrounding marks the graph suspect and forces a fresh `getUserMedia`. But iOS also reclaims the audio session with **no event at all** (Low Power Mode, an idle gap), leaving nothing to flag the corpse — so on the phone the press now reads the analyser once *before* capturing and, if it's flat zero, rebuilds the mic first (the user's words land on a live mic, not a corpse). A mid-dictation AudioContext interruption also freezes the analyser, so the watchdog alarms on a non-running context — and it's never auto-recovered: fail loud, redictate. And a dead/silent capture (peak 0) now fails *named* — "MIC PRODUCED NO SIGNAL" — never the misleading "no speech".
- **Clipboard writes need focus.** Both the modern API and the fallback fail when the tab is unfocused (behind Citrix/Cerner). I don't silently retry, with one sanctioned exception: a pending copy held with a red status and retried on refocus.

## Phone link

For setups where the desktop can't get a good mic. On the desktop, **Pair a phone** shows a QR + 6-char code (QR generated on-page, never an external service — the code is the only credential). The phone joins, switches to a **big-button layout** (one push-to-talk button, whole-screen status, haptics mirroring the beeps), and dictates in batch; the final text POSTs to the desktop, which writes the clipboard. While the phone is dictating, the desktop shows a live **"📱 Phone is recording…"** indicator (a pulsing dot that flips to "transcribing…" on release and clears when the text lands) so I know audio is flowing before the note arrives — it's a relayed, fire-and-forget cue, never load-bearing. Pairing survives reloads and app restarts on both sides until End session (desktop) / Leave (phone). The resilience machinery (heartbeat, reconnect, delivery queue, dedupe, focus-retry, and the AHK `/latest` poller for focus-free pasting) is detailed in `CLAUDE.md`.

## Keyterms

Bias the transcription toward my vocabulary — drugs, anatomy, eponyms, names. Three tiers, merged and deduped per dictation (my terms > checked presets > the always-on standard list), capped at 1000 terms under 50 chars. Edit the lists in `keyterms.js`; they reach every device on the next deploy. Keyterms add ~20% to cost, so the always-on list stays small.

## Settings & storage

Everything is in `localStorage`, scoped to one browser profile / device / origin — desktop, phone, and an installed iOS PWA don't share. Set up each device I dictate from. The split I'm building toward: portable settings (keyterms, append/beep prefs) sync across devices; per-device tuning (gate, hotkey, mic) stays local. Keyterm presets already reach every device because they ship in the code.

## Deploy

Push to `main` → Cloudflare Workers Builds deploys the worker `eleven` (`npx wrangler deploy` also works; the served HTML is `no-store`). Two ways to supply the key:

- **Shared mode:** set `ELEVENLABS_API_KEY` + `APP_PASSPHRASE`. Users enter only the passphrase; the Worker checks it in constant time and injects the key server-side — it never reaches the browser.
- **Bring your own key:** leave the passphrase unset; each user enters their own ElevenLabs key, stored per browser.

Set secrets in the Cloudflare dashboard (the local wrangler token can't write secrets). Install as an app from Chrome/Edge for a standalone window that keeps mic permission and stays usable shrunk to a sliver.

## Tuning the gate

The gate puts a gap between my voice and the room and only lets my voice through. The meter shows the live level with two marks: red = open threshold (speech must exceed it to start recording), yellow = close threshold (holds open until the level drops below it, plus ~0.9 s). Routine: record silence and note where the room sits, set red just above it, confirm the pill flips OPEN when I speak, set yellow low in the gap, then check it stays OPEN through a sentence with pauses.

| Symptom | Fix |
|---|---|
| Word beginnings cut off | Open too high — lower red |
| Words drop mid-sentence | Close too high — lower yellow |
| Background still transcribed | Open too low — raise red |
| Gate flickers open and closed | Gap too narrow — raise red, lower yellow |
| Nothing records | Both above your voice — lower both |
| iPhone records near-empty / "VERY LOW MIC LEVEL" | Low gain: a quiet mic sits below the gate. iOS auto-seeds a modest **Mic gain** (3×) + lower gate on first launch; otherwise raise **Mic gain** and lower red yourself |
| "MIC PRODUCED NO SIGNAL" / "MIC NOT CAPTURING" on the phone | A dead/silent mic (peak 0) — iOS handed back a live-looking but silent stream. The app now **probes every press** and refuses to start recording into a provably silent mic (it rebuilds once, then fails loudly *before* you speak), and an **idle sampler** rebuilds a mic that dies between takes (watch the "Mic ✓ / ⚠" pill). Turn off **Low Power Mode**, and stay in the app between takes (backgrounding is the main trigger) |

A close, low-gain mic does more than the sliders can — and on **iPhone** the app now seeds a modest makeup gain + lower gate automatically on first launch (one-shot, never over a hand-tuned device, never on desktop), because a quiet iPhone mic otherwise records in the *silent dead-band* below the gate. Any residual under-gain now fails **loudly** as "VERY LOW MIC LEVEL" (with the cause + lever) instead of a generic no-speech error. Constants at the top of the client script: `BATCH_UPLOAD_TIMEOUT_MS` (15000, the deadline *floor* — long takes extend it by 25% of the take, capped +60 s), `FLATLINE_RMS` (0.0008, the dead-mic threshold), `IOS_SEED_MIC_GAIN` (3) / `IOS_SEED_GATE_OPEN` (0.018) / `IOS_AUDIO_SEED_VERSION`, `HOTKEY_TAP_MS` (400, tap-vs-hold); the phone-link timings are in `CLAUDE.md`.

**Long dictations**: a multi-minute take can spend up to ~a minute in transcription — the app now waits proportionally instead of aborting at a flat 15 s (the AHK script's `CLIP_TIMEOUT` is sized to match). Every result is also checked against how much speech was actually heard: a transcript that stops well short of the end is flagged **"⚠ Transcript may be INCOMPLETE — covered M:SS of M:SS"** with a warn beep instead of a clean "Done!" — verify the end of the note before pasting.

## Roadmap

In priority order — #1 is the standing rule, never lose a dictation:

1. **Durability — done, now including silent truncation.** The dictation journal landed: each recorded chunk is mirrored to IndexedDB during capture (~30 min cap, and a capped recovery says so), and a take that the app died on (crash / kill / OOM before upload) is recovered on the next boot — a loud banner re-transcribes the saved audio and copies it. The **transcript-coverage guard** closes the last silent gap: a result that stops well short of the observed speech (service truncation or a gate-silenced tail) is a loud "may be INCOMPLETE" warn — never a clean Done! — and logs a diagnosis to the failure ring (`scribe_v2_micfail_v9`). Long takes get a duration-scaled transcription deadline instead of the flat 15 s. *Next:* a UI to browse/replay the failure log and recovered audio.
2. **Settings portability.** Sync the portable settings across devices while keeping per-device tuning local — export/import or passphrase-keyed server-side profiles.
3. **Phone UI.** The big-button layout is done; surfacing the capture waveform on the big screen is the open gap, then a larger live readout and swipe gestures.

## Validation

No browser needed: `node tests/flow.test.mjs` (a jsdom harness simulating full dictation sessions + the phone link) plus a `node --check` of the served script. Update the scenarios in the same change as any session-flow / beep / clipboard / watchdog change. The full developer guide is `CLAUDE.md`.
