#Requires AutoHotkey v2.0
#SingleInstance Force
SetCapsLockState("AlwaysOff")
SendMode("Event")              ; reliable while CapsLock is physically held
SetKeyDelay(0, 10)

; ===== CONFIG =====
SPINUP         := 80           ; ms before speaking (mic is kept warm by the browser)
MIN_HOLD       := 200          ; taps shorter than this are discarded. Short-form dictation
                               ; ("yes", "normal", "negative") can finish inside 350 ms, and
                               ; the old value silently refused to paste those real takes.
CLIP_TIMEOUT   := 90           ; backstop; must cover the duration-aware batch deadline (15s floor + up to 60s extra on a long take ≈ 75s worst case, plus the browser's sub-second capture tail); sentinel makes failures return in ~2s
ACT_TIMEOUT    := 0.15         ; per-attempt window-activation wait
ACT_TRIES      := 2
HOLD_CAP_MS    := 60000        ; absolute max hold before we force-stop (anti-wedge)
STRIP_NEWLINES := true
TRAILING_SPACE := true
ERR_SOUND      := true         ; the ONLY sound this script makes — on failure
SENTINEL       := "##DICTATION_FAILED##"   ; must match the browser's marker

; --- Focus return (the single biggest latency win) ---
; Dictating STEALS focus: the script activates the dictation window to send
; F13/F14 and to let the browser write the clipboard (a clipboard write needs
; document focus). Without the return below, every dictation ended with you
; sitting in the dictation window having to alt-tab back to Cerner — 1-3 s of
; real time per take, larger than anything in the transcription pipeline.
RETURN_FOCUS   := true         ; re-activate whatever window you were in when you pressed

; AUTO_PASTE sends Ctrl+V once focus is back. OFF BY DEFAULT ON PURPOSE: it
; only fires when focus was restored to the EXACT window handle captured at
; press time and the result was not the failure sentinel, but the caret inside
; that window may still have moved, and a paste into the wrong field is a
; wrong-chart risk. Turn it on only once you trust your own workflow.
AUTO_PASTE     := false

; --- On-screen state bar (works with no speakers, and when the PWA is hidden) ---
; A small always-on-top strip that never takes focus (WS_EX_NOACTIVATE) and
; passes clicks through (WS_EX_TRANSPARENT). This is the cue that still works
; when the dictation window is completely covered by Cerner.
SHOW_STATE_BAR := true
STATE_BAR_W    := 250
STATE_BAR_H    := 38
STATE_BAR_MARGIN := 18         ; distance from the bottom-right screen corner

; --- Phone-link clipboard poller (optional) ---
; The browser can only write the clipboard while its window is focused. If you
; dictate on a phone into this machine, set PHONE_POLL_URL to the worker and
; PHONE_CODE to the session code shown on the desktop page: this script then
; fetches the latest phone delivery and writes it to the clipboard natively —
; no browser focus needed. Leave PHONE_CODE empty to disable.
PHONE_POLL_URL := ""           ; e.g. "https://eleven.example.workers.dev" (no trailing slash)
PHONE_CODE     := ""           ; 6-char session code from the desktop page
PHONE_AUTH     := ""           ; access code (same one the app's Access section uses) —
                               ; REQUIRED when the worker runs in shared mode: the session
                               ; routes answer 401 without it. Leave empty for a BYO-key
                               ; deploy with no APP_PASSPHRASE set.
PHONE_POLL_MS  := 2000
; ==================

DICT_HWND := 0
BUSY      := false
PREV_HWND := 0                 ; the window you were in when the press started

; ===== On-screen state bar =====
StateGui := ""
StateTxt := ""

InitStateBar() {
    global StateGui, StateTxt, SHOW_STATE_BAR, STATE_BAR_W, STATE_BAR_H
    if !SHOW_STATE_BAR
        return
    ; +E0x08000000 = WS_EX_NOACTIVATE — showing this must NEVER pull focus off
    ; Cerner mid-dictation. +E0x00000020 = WS_EX_TRANSPARENT — clicks fall
    ; through to whatever is underneath.
    StateGui := Gui("+AlwaysOnTop -Caption +ToolWindow +E0x08000000 +E0x00000020 -DPIScale", "Dictation state")
    StateGui.MarginX := 0
    StateGui.MarginY := 0
    StateGui.SetFont("s13 bold cWhite", "Segoe UI")
    StateTxt := StateGui.Add("Text", "w" STATE_BAR_W " h" STATE_BAR_H " Center +0x200", "")
}

ShowState(msg, bg) {
    global StateGui, StateTxt, SHOW_STATE_BAR, STATE_BAR_W, STATE_BAR_H, STATE_BAR_MARGIN
    if (!SHOW_STATE_BAR || !IsObject(StateGui))
        return
    try {
        StateGui.BackColor := bg
        StateTxt.Value := msg
        x := A_ScreenWidth  - STATE_BAR_W - STATE_BAR_MARGIN
        y := A_ScreenHeight - STATE_BAR_H - STATE_BAR_MARGIN
        ; NoActivate keeps the caret and the foreground window exactly where they are.
        StateGui.Show("NoActivate x" x " y" y " w" STATE_BAR_W " h" STATE_BAR_H)
    }
}

HideState() {
    global StateGui, SHOW_STATE_BAR
    if (!SHOW_STATE_BAR || !IsObject(StateGui))
        return
    try StateGui.Hide()
}

; Show a state, then auto-hide it after ms (negative SetTimer period = run once).
FlashState(msg, bg, ms) {
    ShowState(msg, bg)
    SetTimer(HideState, -ms)
}

InitStateBar()

; ===== Phone-link poller =====
LAST_DELIVERY_ID := ""
POLL_SEEDED      := false      ; first poll only baselines — never paste a pre-existing (possibly stale) delivery

if (PHONE_POLL_URL != "" && PHONE_CODE != "")
    SetTimer(PollPhoneDelivery, PHONE_POLL_MS)

PollPhoneDelivery() {
    global PHONE_POLL_URL, PHONE_CODE, PHONE_AUTH, LAST_DELIVERY_ID, POLL_SEEDED
    global BUSY, STRIP_NEWLINES, TRAILING_SPACE
    static polling := false
    if (polling || BUSY)               ; never fight the PTT clipboard handshake
        return
    polling := true
    try {
        req := ComObject("WinHttp.WinHttpRequest.5.1")
        req.Open("GET", PHONE_POLL_URL "/api/session/" PHONE_CODE "/latest", true)
        if (PHONE_AUTH != "")
            req.SetRequestHeader("x-phone-auth", PHONE_AUTH)
        req.Send()
        req.WaitForResponse(5)
        body := req.ResponseText
        if (RegExMatch(body, '"delivery_id"\s*:\s*"((?:[^"\\]|\\.)*)"', &mId)
            && RegExMatch(body, '"text"\s*:\s*"((?:[^"\\]|\\.)*)"', &mTxt)
            && mId[1] != "") {
            if !POLL_SEEDED {
                ; A delivery may be held from before this script started —
                ; baseline its id so we only ever copy NEW dictations.
                LAST_DELIVERY_ID := mId[1]
            } else if (mId[1] != LAST_DELIVERY_ID) {
                LAST_DELIVERY_ID := mId[1]
                txt := JsonUnescape(mTxt[1])
                if STRIP_NEWLINES
                    txt := RegExReplace(txt, "\R+", " ")
                txt := Trim(RegExReplace(txt, " +", " "))
                if (txt != "") {
                    if TRAILING_SPACE
                        txt .= " "
                    A_Clipboard := txt
                    Notify("Phone transcript on clipboard.")
                }
            }
        }
        POLL_SEEDED := true
    } catch {
        ; network blip — the next poll retries; stay silent (the browser side
        ; of the link is the loud one)
    }
    polling := false
}

JsonUnescape(s) {
    s := StrReplace(s, "\\", Chr(1))   ; protect escaped backslashes first
    s := StrReplace(s, '\"', '"')
    s := StrReplace(s, "\n", "`n")
    s := StrReplace(s, "\r", "`r")
    s := StrReplace(s, "\t", A_Tab)
    s := StrReplace(s, "\/", "/")
    while RegExMatch(s, "\\u([0-9A-Fa-f]{4})", &m)
        s := StrReplace(s, m[0], Chr(Integer("0x" m[1])))
    return StrReplace(s, Chr(1), "\")
}

; --- Feedback: toasts are ALWAYS silent (option 16). The only audio is ErrBeep
;     on a genuine failure. Success is completely silent. ---
Notify(msg) {
    TrayTip(msg, "Dictation", 16)          ; 16 = no notification sound
}
ErrBeep() {
    global ERR_SOUND
    if ERR_SOUND
        SoundPlay("*16")
}
Fail(msg) {
    ErrBeep()
    Notify(msg)
}

ActivateWindow(hwnd) {
    global ACT_TIMEOUT, ACT_TRIES
    if (!hwnd || !WinExist("ahk_id " hwnd))
        return false
    if WinActive("ahk_id " hwnd)           ; already focused — skip the wait
        return true
    Loop ACT_TRIES {
        try WinActivate("ahk_id " hwnd)
        if WinWaitActive("ahk_id " hwnd, , ACT_TIMEOUT)
            return true
    }
    return WinActive("ahk_id " hwnd) ? true : false
}

; ===== Register the dictation window: focus it, press Win+F12 =====
; Silent confirmation toast, no sound.
#F12::
{
    global DICT_HWND
    DICT_HWND := WinActive("A")
    Notify("Dictation window registered.")
}

; ===== Push-to-talk: CAPSLOCK HOLD → RECORD → CLEANED TEXT ON CLIPBOARD =====
*CapsLock::
{
    global DICT_HWND, BUSY, SPINUP, MIN_HOLD, CLIP_TIMEOUT, HOLD_CAP_MS
    global STRIP_NEWLINES, TRAILING_SPACE, SENTINEL
    global PREV_HWND, RETURN_FOCUS, AUTO_PASTE

    if BUSY {
        KeyWait("CapsLock")
        return
    }
    if (!DICT_HWND || !WinExist("ahk_id " DICT_HWND)) {
        Fail("Register dictation window first (Win+F12).")
        KeyWait("CapsLock")
        return
    }

    BUSY := true
    A_Clipboard := ""

    ; Remember where you were BEFORE stealing focus — this is what makes the
    ; return (and the optional auto-paste) land in the right place.
    PREV_HWND := WinActive("A")

    if !ActivateWindow(DICT_HWND) {
        Fail("Could not focus the dictation window.")
        BUSY := false
        KeyWait("CapsLock")
        return
    }

    Send("{F13}")
    ShowState("● RECORDING", "B91C1C")
    Sleep(SPINUP)

    ; --- Wait for release using PHYSICAL key state (robust against competing
    ;     keyboard hooks that can swallow a logical key-up). Hard cap prevents
    ;     ever wedging here forever. ---
    startTick := A_TickCount
    Loop {
        if !GetKeyState("CapsLock", "P")
            break
        Sleep(15)
        if (A_TickCount - startTick > HOLD_CAP_MS)
            break
    }
    heldMs := A_TickCount - startTick

    ; --- Stop recording. Re-activate first so a focus drift can't skip F14,
    ;     and send twice as cheap insurance (browser ignores the 2nd if stopped). ---
    ActivateWindow(DICT_HWND)
    Send("{F14}")
    Sleep(20)
    Send("{F14}")

    ; --- Discard accidental taps ---
    if (heldMs < MIN_HOLD) {
        FlashState("TAP IGNORED", "7C2D12", 1200)
        Fail("Tap ignored (hold to dictate).")
        ReturnFocus()
        BUSY := false
        return
    }

    ; The browser is uploading and transcribing; the clipboard write is the
    ; finish line. Keep the dictation window focused until then — the write
    ; requires document focus.
    ShowState("TRANSCRIBING…", "B45309")

    ; --- Wait for transcript OR failure sentinel on the clipboard ---
    if !ClipWait(CLIP_TIMEOUT, 1) {
        FlashState("⚠ TIMED OUT", "B91C1C", 2500)
        Fail("No transcript (timeout).")
        ReturnFocus()
        BUSY := false
        return
    }

    txt := A_Clipboard

    ; Browser signalled a failed / empty transcription — bail fast, don't paste.
    if (txt = SENTINEL) {
        A_Clipboard := ""
        FlashState("⚠ FAILED", "B91C1C", 2500)
        Fail("No speech detected / transcription failed.")
        ReturnFocus()
        BUSY := false
        return
    }

    cleaned := txt
    if STRIP_NEWLINES
        cleaned := RegExReplace(cleaned, "\R+", " ")
    cleaned := Trim(RegExReplace(cleaned, " +", " "))
    if (cleaned = "") {
        A_Clipboard := ""
        FlashState("⚠ EMPTY", "B91C1C", 2500)
        Fail("Empty transcript.")
        ReturnFocus()
        BUSY := false
        return
    }
    if TRAILING_SPACE
        cleaned .= " "

    ; The browser's cleanTranscript already strips newlines, collapses spaces and
    ; appends the trailing space, so the rewrite is usually a no-op. Skipping it
    ; when nothing changed saves a clipboard round trip AND its ClipWait.
    if (cleaned != txt) {
        A_Clipboard := cleaned
        ClipWait(2, 1)
    }

    ; SUCCESS: silent (the browser's done-beep already sounded). Hand focus back
    ; so the dictation does not end with an alt-tab, and paste only if that
    ; landed on the exact window the press started in.
    pasted := ReturnFocus()
    if (AUTO_PASTE && pasted)
        Send("^v")
    FlashState(AUTO_PASTE && pasted ? "✓ PASTED" : "✓ READY", "15803D", 900)
    BUSY := false
}

; Re-activate the window that was focused when the press began. Returns true
; only if that exact window is now foreground — the guard the auto-paste needs.
ReturnFocus() {
    global PREV_HWND, DICT_HWND, RETURN_FOCUS
    if (!RETURN_FOCUS || !PREV_HWND || PREV_HWND = DICT_HWND)
        return false
    if !WinExist("ahk_id " PREV_HWND)
        return false
    ActivateWindow(PREV_HWND)
    return WinActive("ahk_id " PREV_HWND) ? true : false
}

; Shift+CapsLock toggles real caps lock if ever needed
+CapsLock::SetCapsLockState(!GetKeyState("CapsLock", "T"))

; Emergency reload (also clears a stuck BUSY state)
^!q::Reload()
