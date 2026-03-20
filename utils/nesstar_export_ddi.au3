#include <WinAPI.au3>

Opt("WinTitleMatchMode", 2)

If $CmdLine[0] < 2 Then
    Exit 2
EndIf

Local $outputDir = $CmdLine[1]
Local $fileName = $CmdLine[2]

Func _Log($s)
    ConsoleWrite("LOG " & $s & @CRLF)
EndFunc

Func _Stage($s)
    ConsoleWrite("STAGE " & $s & @CRLF)
EndFunc

Func _FindAnyDialogHandle()
    Local $list = WinList("[CLASS:#32770]")
    If @error Then Return ""
    If $list[0][0] < 1 Then Return ""
    Local $i
    For $i = 1 To $list[0][0]
        Local $h = $list[$i][1]
        If $h = "" Then ContinueLoop
        If Not WinExists($h) Then ContinueLoop
        If ControlGetHandle($h, "", "Edit1") <> "" And ControlGetHandle($h, "", "Button1") <> "" Then
            Return $h
        EndIf
    Next
    Return ""
EndFunc

Func _DismissActiveModal()
    If WinActive("[CLASS:#32770]") = 0 Then Return False
    Send("{ESC}")
    Sleep(250)
    Return True
EndFunc

Func WaitForNesstar()
    For $i = 1 To 10
        _DismissActiveModal()
        Local $hWnd = WinGetHandle("[REGEXPTITLE:(?i)nesstar]")
        If $hWnd <> "" Then
            WinActivate($hWnd)
            WinWaitActive($hWnd, "", 5)
            Return $hWnd
        EndIf
        Sleep(1000)
    Next
    Exit 10
EndFunc

Func _IsSaveAsTitleLower($t)
    If $t = "" Then Return False
    If StringInStr($t, "save") > 0 Then Return True
    If StringInStr($t, "export") > 0 Then Return True
    Return False
EndFunc

Func _FindSaveAsDialogHandle()
    Local $list = WinList("[CLASS:#32770]")
    If @error Then Return ""
    If $list[0][0] < 1 Then Return ""

    Local $i
    For $i = 1 To $list[0][0]
        Local $h = $list[$i][1]
        If $h = "" Then ContinueLoop
        If Not WinExists($h) Then ContinueLoop
        If ControlGetHandle($h, "", "Edit1") = "" Then ContinueLoop
        Local $title = StringLower(StringStripWS(WinGetTitle($h), 3))
        If _IsSaveAsTitleLower($title) Then Return $h
    Next
    Return ""
EndFunc

Func _WaitForSaveAsDialogHandle($timeoutSec)
    Local $t = TimerInit()
    While TimerDiff($t) < ($timeoutSec * 1000)
        Local $h = _FindSaveAsDialogHandle()
        If $h <> "" Then
            WinActivate($h)
            WinWaitActive($h, "", 10)
            Return $h
        EndIf
        _DismissActiveModal()
    WEnd
    Return ""
EndFunc

Func _ClickSaveButton($hDlg)
    Local $i
    For $i = 1 To 12
        Local $ctrl = "Button" & $i
        If ControlGetHandle($hDlg, "", $ctrl) = "" Then ContinueLoop
        Local $txt = StringLower(StringStripWS(ControlGetText($hDlg, "", $ctrl), 3))
        If $txt = "" Then ContinueLoop
        If StringInStr($txt, "save") > 0 Or StringInStr($txt, "export") > 0 Or StringInStr($txt, "ok") > 0 Then
            ControlClick($hDlg, "", $ctrl)
            Return True
        EndIf
    Next
    Send("{ENTER}")
    Return True
EndFunc

Func _FindOverwriteDialogHandle()
    Local $list = WinList("[CLASS:#32770]")
    If @error Then Return ""
    If $list[0][0] < 1 Then Return ""

    Local $i
    For $i = 1 To $list[0][0]
        Local $h = $list[$i][1]
        If $h = "" Then ContinueLoop
        If Not WinExists($h) Then ContinueLoop
        If ControlGetHandle($h, "", "Edit1") <> "" Then ContinueLoop

        Local $title = StringLower(StringStripWS(WinGetTitle($h), 3))
        If $title = "" Then ContinueLoop
        If StringInStr($title, "confirm save as") = 0 And StringInStr($title, "replace") = 0 And StringInStr($title, "confirm") = 0 And StringInStr($title, "already exists") = 0 And StringInStr($title, "overwrite") = 0 Then
            ContinueLoop
        EndIf
        Return $h
    Next
    Return ""
EndFunc

Func _ConfirmOverwriteDialog($hDlg)
    If $hDlg = "" Then Return False
    WinActivate($hDlg)
    WinWaitActive($hDlg, "", 5)
    Local $i
    For $i = 1 To 12
        Local $ctrl = "Button" & $i
        If ControlGetHandle($hDlg, "", $ctrl) = "" Then ContinueLoop
        Local $txt = StringLower(StringStripWS(ControlGetText($hDlg, "", $ctrl), 3))
        If $txt = "" Then ContinueLoop
        If StringInStr($txt, "yes") > 0 Or StringInStr($txt, "replace") > 0 Or StringInStr($txt, "overwrite") > 0 Then
            ControlClick($hDlg, "", $ctrl)
            Return True
        EndIf
    Next
    Send("{LEFT}{ENTER}")
    Return True
EndFunc

Func _PrepareAndWaitForManualSave($fullPath, $timeoutSec)
    ; Wait for the Save As dialog to appear
    Local $hDlg = _WaitForSaveAsDialogHandle(30)
    If $hDlg = "" Then Return False

    ; Pre-fill the filename so the user only needs to click Save
    ControlFocus($hDlg, "", "Edit1")
    ControlSetText($hDlg, "", "Edit1", $fullPath)
    ControlSend($hDlg, "", "Edit1", "{END}") ; Move cursor to end so path is visible

    _Log("DDI Save dialog pre-filled with: " & $fullPath)
    _Log("Waiting for user to manually click Save...")

    ; Now just wait for the dialog to close (user clicks Save)
    Local $t = TimerInit()
    While TimerDiff($t) < ($timeoutSec * 1000)
        ; Handle overwrite confirmation if user saves to existing file
        Local $ovr = _FindOverwriteDialogHandle()
        If $ovr <> "" Then
            _ConfirmOverwriteDialog($ovr)
            WinWaitClose($ovr, "", 10)
            ContinueLoop
        EndIf
        ; Dialog closed = user clicked Save
        If WinWaitClose($hDlg, "", 1) Then
            _Log("DDI Save dialog closed by user.")
            Return True
        EndIf
    WEnd
    _Log("Timed out waiting for user to save DDI dialog.")
    Return False
EndFunc

Local $hMain = WaitForNesstar()

_Log("Activating Nesstar")
WinActivate($hMain)
WinWaitActive($hMain, "", 10)

_Stage("EXPORTING_METADATA_DDI")

; Trigger the DDI export dialog — the user will manually select location and click Save.
; AutoIt does NOT interact with the Save dialog at all.
Send("^!e") ; Ctrl + Alt + E

_Log("DDI export dialog triggered. Waiting briefly to ensure it opened...")
Sleep(2000) ; Give the dialog 2 seconds to fully appear

_Log("AutoIt DDI script done. Python will detect the XML file when user saves.")
Exit 0


Local $savePath = $outputDir & "\\" & $fileName
; Open the Save dialog, pre-fill the path, then wait for user to click Save manually
Local $saveHandled = _PrepareAndWaitForManualSave($savePath, 300) ; 5-minute timeout

If Not $saveHandled Then Exit 20

_Log("DDI exported successfully: " & $savePath)

; Close Nesstar after DDI is saved
WinActivate($hMain)
WinWaitActive($hMain, "", 5)
Send("!f")
Send("x")

Exit 0
