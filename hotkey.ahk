#Requires AutoHotkey v2.0
#SingleInstance Force
SetCapsLockState("AlwaysOff")
SendMode("Event")              ; reliable while CapsLock is physically held
SetKeyDelay(0, 10)

; ===== CONFIG =====
SPINUP         := 80           ; ms before speaking (mic is kept warm by the browser)
MIN_HOLD       := 350          ; taps shorter than this are discarded
CLIP_TIMEOUT   := 90           ; backstop; must cover the duration-aware batch deadline (15s floor + up to 60s extra on a long take ≈ 75s worst case); sentinel makes failures return in ~2s
ACT_TIMEOUT    := 0.15         ; per-attempt window-activation wait
ACT_TRIES      := 2
HOLD_CAP_MS    := 60000        ; absolute max hold before we force-stop (anti-wedge)
STRIP_NEWLINES := true
TRAILING_SPACE := true
ERR_SOUND      := true         ; the ONLY sound this script makes — on failure
SENTINEL       := "##DICTATION_FAILED##"   ; must match the browser's marker

; --- Phone-link clipboard poller (optional) ---
; The browser can only write the clipboard while its window is focused. If you
; dictate on a phone into this machine, set PHONE_POLL_URL to the worker and
; PHONE_CODE to the session code shown on the desktop page: this script then
; fetches the latest phone delivery and writes it to the clipboard natively —
; no browser focus needed. Leave PHONE_CODE empty to disable.
PHONE_POLL_URL := ""           ; e.g. "https://eleven.example.workers.dev" (no trailing slash)
PHONE_CODE     := ""           ; 6-char session code from the desktop page
PHONE_POLL_MS  := 2000
; ==================

DICT_HWND := 0
BUSY      := false

; ===== Phone-link poller =====
LAST_DELIVERY_ID := ""
POLL_SEEDED      := false      ; first poll only baselines — never paste a pre-existing (possibly stale) delivery

if (PHONE_POLL_URL != "" && PHONE_CODE != "")
    SetTimer(PollPhoneDelivery, PHONE_POLL_MS)

PollPhoneDelivery() {
    global PHONE_POLL_URL, PHONE_CODE, LAST_DELIVERY_ID, POLL_SEEDED
    global BUSY, STRIP_NEWLINES, TRAILING_SPACE
    static polling := false
    if (polling || BUSY)               ; never fight the PTT clipboard handshake
        return
    polling := true
    try {
        req := ComObject("WinHttp.WinHttpRequest.5.1")
        req.Open("GET", PHONE_POLL_URL "/api/session/" PHONE_CODE "/latest", true)
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

    if !ActivateWindow(DICT_HWND) {
        Fail("Could not focus the dictation window.")
        BUSY := false
        KeyWait("CapsLock")
        return
    }

    Send("{F13}")
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
        Fail("Tap ignored (hold to dictate).")
        BUSY := false
        return
    }

    ; --- Wait for transcript OR failure sentinel on the clipboard ---
    if !ClipWait(CLIP_TIMEOUT, 1) {
        Fail("No transcript (timeout).")
        BUSY := false
        return
    }

    txt := A_Clipboard

    ; Browser signalled a failed / empty transcription — bail fast, don't paste.
    if (txt = SENTINEL) {
        A_Clipboard := ""
        Fail("No speech detected / transcription failed.")
        BUSY := false
        return
    }

    if STRIP_NEWLINES
        txt := RegExReplace(txt, "\R+", " ")
    txt := Trim(RegExReplace(txt, " +", " "))
    if (txt = "") {
        A_Clipboard := ""
        Fail("Empty transcript.")
        BUSY := false
        return
    }
    if TRAILING_SPACE
        txt .= " "

    A_Clipboard := txt
    ClipWait(2, 1)

    ; SUCCESS: completely silent. The browser's done-beep already told you it's
    ; ready; cleaned text is on the clipboard waiting for Ctrl+V.
    BUSY := false
}

; Shift+CapsLock toggles real caps lock if ever needed
+CapsLock::SetCapsLockState(!GetKeyState("CapsLock", "T"))

; Emergency reload (also clears a stuck BUSY state)
^!q::Reload()
