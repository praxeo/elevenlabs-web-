#Requires AutoHotkey v2.0
#SingleInstance Force

SetCapsLockState("AlwaysOff")
SendMode("Event")
SetKeyDelay(0, 10)

; ===== CONFIG =====
SPINUP         := 70           ; ms before speaking
MIN_HOLD       := 200          ; taps shorter than this are discarded
CLIP_TIMEOUT   := 90           ; covers duration-aware batch transcription
ACT_TIMEOUT    := 0.15         ; per-attempt window activation wait
ACT_TRIES      := 2            ; activation attempts
HOLD_CAP_MS    := 60000        ; force-stop after 60 seconds (anti-wedge)
STRIP_NEWLINES := true
TRAILING_SPACE := true
ERR_SOUND      := true         ; only sound made by this script: failure
SENTINEL       := "##DICTATION_FAILED##"
DBLCLICK_MS    := 300          ; back-button double-press window
COPY_DELAY_MS  := 30           ; delay between Ctrl+A and Ctrl+C
PASTE_DELAY_MS := 100          ; delay before Ctrl+V after clicking

; Return to the window that was active when dictation began. Automatic paste
; remains off because restoring the correct top-level window does not prove
; that the caret is still in the intended field.
RETURN_FOCUS   := true
AUTO_PASTE     := false

; Always-on-top state bar that cannot take focus and passes clicks through.
SHOW_STATE_BAR   := true
STATE_BAR_W      := 250
STATE_BAR_H      := 38
STATE_BAR_MARGIN := 18

; Optional phone-link clipboard poller. Leave PHONE_CODE empty to disable it.
PHONE_POLL_URL := ""           ; e.g. "https://eleven.example.workers.dev"
PHONE_CODE     := ""           ; 6-character session code
PHONE_AUTH     := ""           ; required for shared-mode deployments
PHONE_POLL_MS  := 2000
; ==================

DICT_HWND  := 0
PREV_HWND  := 0
BUSY       := false
X1_WAITING := false


; ===== On-screen state bar =====

StateGui := ""
StateTxt := ""

InitStateBar() {
    global StateGui, StateTxt, SHOW_STATE_BAR, STATE_BAR_W, STATE_BAR_H

    if !SHOW_STATE_BAR
        return

    ; WS_EX_NOACTIVATE + WS_EX_TRANSPARENT: never takes focus and clicks pass
    ; through to the application underneath it.
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
        x := A_ScreenWidth - STATE_BAR_W - STATE_BAR_MARGIN
        y := A_ScreenHeight - STATE_BAR_H - STATE_BAR_MARGIN
        StateGui.Show("NoActivate x" x " y" y " w" STATE_BAR_W " h" STATE_BAR_H)
    }
}

HideState() {
    global StateGui, SHOW_STATE_BAR

    if (!SHOW_STATE_BAR || !IsObject(StateGui))
        return

    try StateGui.Hide()
}

FlashState(msg, bg, ms) {
    ShowState(msg, bg)
    SetTimer(HideState, -ms)
}

InitStateBar()


; ===== Phone-link poller =====

LAST_DELIVERY_ID := ""
POLL_SEEDED      := false

if (PHONE_POLL_URL != "" && PHONE_CODE != "")
    SetTimer(PollPhoneDelivery, PHONE_POLL_MS)

PollPhoneDelivery() {
    global PHONE_POLL_URL, PHONE_CODE, PHONE_AUTH, LAST_DELIVERY_ID, POLL_SEEDED
    global BUSY, STRIP_NEWLINES, TRAILING_SPACE
    static polling := false

    if (polling || BUSY)
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
                ; Baseline the first delivery so stale text is never copied.
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
        ; Retry silently on the next polling interval.
    }

    polling := false
}

JsonUnescape(s) {
    s := StrReplace(s, "\\", Chr(1))
    s := StrReplace(s, '\"', '"')
    s := StrReplace(s, "\n", "`n")
    s := StrReplace(s, "\r", "`r")
    s := StrReplace(s, "\t", A_Tab)
    s := StrReplace(s, "\/", "/")

    while RegExMatch(s, "\\u([0-9A-Fa-f]{4})", &m)
        s := StrReplace(s, m[0], Chr(Integer("0x" m[1])))

    return StrReplace(s, Chr(1), "\")
}


; ===== Notifications =====

Notify(msg) {
    TrayTip(msg, "Dictation", 16)
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


; ===== Window activation =====

ActivateWindow(hwnd) {
    global ACT_TIMEOUT, ACT_TRIES

    if (!hwnd || !WinExist("ahk_id " hwnd))
        return false

    if WinActive("ahk_id " hwnd)
        return true

    Loop ACT_TRIES {
        try WinActivate("ahk_id " hwnd)

        if WinWaitActive("ahk_id " hwnd, , ACT_TIMEOUT)
            return true
    }

    return WinActive("ahk_id " hwnd) ? true : false
}

ReturnFocus() {
    global PREV_HWND, DICT_HWND, RETURN_FOCUS

    if (!RETURN_FOCUS || !PREV_HWND || PREV_HWND = DICT_HWND)
        return false

    if !WinExist("ahk_id " PREV_HWND)
        return false

    ActivateWindow(PREV_HWND)
    return WinActive("ahk_id " PREV_HWND) ? true : false
}


; ===== Register dictation window =====
; Focus the dictation browser window and press Win+F12.

#F12::
{
    global DICT_HWND

    DICT_HWND := WinActive("A")
    Notify("Dictation window registered.")
}


; ===== Push-to-talk =====
; Hold XButton2 to record. Release XButton2 to stop.
; Cleaned transcription is placed on the clipboard.

*XButton2::
{
    global DICT_HWND, PREV_HWND, BUSY
    global SPINUP, MIN_HOLD, CLIP_TIMEOUT, HOLD_CAP_MS
    global STRIP_NEWLINES, TRAILING_SPACE, SENTINEL
    global RETURN_FOCUS, AUTO_PASTE

    if BUSY {
        KeyWait("XButton2")
        return
    }

    if (!DICT_HWND || !WinExist("ahk_id " DICT_HWND)) {
        Fail("Register dictation window first (Win+F12).")
        KeyWait("XButton2")
        return
    }

    BUSY := true
    A_Clipboard := ""

    ; Capture the originating window before activating the dictation browser.
    PREV_HWND := WinActive("A")

    if !ActivateWindow(DICT_HWND) {
        Fail("Could not focus the dictation window.")
        BUSY := false
        KeyWait("XButton2")
        return
    }

    Send("{F13}")
    ShowState(Chr(0x25CF) " RECORDING", "B91C1C")
    Sleep(SPINUP)

    startTick := A_TickCount

    Loop {
        if !GetKeyState("XButton2", "P")
            break

        Sleep(15)

        if (A_TickCount - startTick > HOLD_CAP_MS)
            break
    }

    heldMs := A_TickCount - startTick

    ; Re-activate before F14 in case focus drifted while dictating. The browser
    ; ignores the second F14 if recording already stopped.
    ActivateWindow(DICT_HWND)
    Send("{F14}")
    Sleep(20)
    Send("{F14}")

    if (heldMs < MIN_HOLD) {
        FlashState("TAP IGNORED", "7C2D12", 1200)
        Fail("Tap ignored (hold to dictate).")
        ReturnFocus()
        BUSY := false
        return
    }

    ShowState("TRANSCRIBING" Chr(0x2026), "B45309")

    if !ClipWait(CLIP_TIMEOUT, 1) {
        FlashState(Chr(0x26A0) " TIMED OUT", "B91C1C", 2500)
        Fail("No transcript (timeout).")
        ReturnFocus()
        BUSY := false
        return
    }

    txt := A_Clipboard

    if (txt = SENTINEL) {
        A_Clipboard := ""
        FlashState(Chr(0x26A0) " FAILED", "B91C1C", 2500)
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
        FlashState(Chr(0x26A0) " EMPTY", "B91C1C", 2500)
        Fail("Empty transcript.")
        ReturnFocus()
        BUSY := false
        return
    }

    if TRAILING_SPACE
        cleaned .= " "

    ; Avoid an unnecessary clipboard rewrite if the browser already supplied
    ; text in final form.
    if (cleaned != txt) {
        A_Clipboard := cleaned
        ClipWait(2, 1)
    }

    focusRestored := ReturnFocus()

    if (AUTO_PASTE && focusRestored)
        SendEvent("^v")

    FlashState(AUTO_PASTE && focusRestored ? Chr(0x2713) " PASTED" : Chr(0x2713) " READY", "15803D", 900)
    BUSY := false
}


; ===== Back-button single-press timer =====
; Fires Enter only if a second press does not arrive.

FireEnter() {
    global X1_WAITING

    X1_WAITING := false
    SendEvent("{Enter}")
}


; ===== Back button + right-click =====
; Select all, then copy.

XButton1 & RButton::
{
    global BUSY, X1_WAITING, COPY_DELAY_MS

    if BUSY
        return

    X1_WAITING := false
    SetTimer(FireEnter, 0)

    SendEvent("^a")
    Sleep(COPY_DELAY_MS)
    SendEvent("^c")

    KeyWait("RButton")
    KeyWait("XButton1")
}


; ===== Mouse back button =====
; Single press: Enter. Double press: left-click, then paste.

*XButton1::
{
    global BUSY, X1_WAITING, DBLCLICK_MS, PASTE_DELAY_MS

    if BUSY
        return

    if X1_WAITING {
        X1_WAITING := false
        SetTimer(FireEnter, 0)

        Click
        Sleep(PASTE_DELAY_MS)
        SendEvent("^v")
        return
    }

    X1_WAITING := true
    SetTimer(FireEnter, -DBLCLICK_MS)
}


; Shift+CapsLock toggles real Caps Lock if it is ever needed.

+CapsLock::SetCapsLockState(!GetKeyState("CapsLock", "T"))


; ===== Emergency reload =====
; Also clears any stuck BUSY or timer state.

^!q::Reload()
