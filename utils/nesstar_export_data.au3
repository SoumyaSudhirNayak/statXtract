#include <WinAPI.au3>

Opt("WinTitleMatchMode", 2)

If $CmdLine[0] < 2 Then
    Exit 2
EndIf

Local $studyPath = $CmdLine[1]
Local $outputDir = $CmdLine[2]
Local $titleHint = ""
If $CmdLine[0] >= 3 Then
    $titleHint = $CmdLine[3]
EndIf

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
    For $i = 1 To 30
        _DismissActiveModal()
        Local $hWnd = WinGetHandle("[REGEXPTITLE:(?i)nesstar]")
        If $hWnd <> "" Then
            WinActivate($hWnd)
            WinWaitActive($hWnd, "", 5)
            Return $hWnd
        EndIf
        Sleep(2000)
    Next
    Exit 10
EndFunc

Func OpenStudy()
    Local $attempt
    For $attempt = 1 To 5
        WinActivate("[REGEXPTITLE:(?i)nesstar]")
        WinWaitActive("[REGEXPTITLE:(?i)nesstar]", "", 5)
        Send("!f")
        Sleep(500)
        Send("o")
        Sleep(1200)

        Local $i
        For $i = 1 To 20
            Local $dlg = _FindAnyDialogHandle()
            If $dlg <> "" Then
                _Log("File-open dialog detected")
                WinActivate($dlg)
                WinWaitActive($dlg, "", 5)
                ControlSetText($dlg, "", "Edit1", $studyPath)
                Sleep(500)
                ControlSend($dlg, "", "Edit1", "{ENTER}")
                Return
            EndIf
            _DismissActiveModal()
            Sleep(1000)
        Next

        _Log("Retrying file-open dialog detection")
    Next
    Exit 11
EndFunc

Func _IsSaveAsTitleLower($t)
    If $t = "" Then Return False
    If StringInStr($t, "save as") > 0 Then Return True
    If StringInStr($t, "save dataset") > 0 Then Return True
    If StringInStr($t, "export dataset") > 0 Then Return True
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
        WinWaitActive("[REGEXPTITLE:(?i)(save as|save dataset|export dataset)]", "", 1)
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

Func _ClickPrimaryDialogButton($hDlg)
    Local $i
    For $i = 1 To 8
        Local $ctrl = "Button" & $i
        If ControlGetHandle($hDlg, "", $ctrl) = "" Then ContinueLoop
        Local $txt = StringLower(StringStripWS(ControlGetText($hDlg, "", $ctrl), 3))
        If $txt = "" Then ContinueLoop
        If StringInStr($txt, "ok") > 0 Or StringInStr($txt, "next") > 0 Or StringInStr($txt, "finish") > 0 Or StringInStr($txt, "export") > 0 Or StringInStr($txt, "save") > 0 Then
            ControlClick($hDlg, "", $ctrl)
            Sleep(250)
            Return True
        EndIf
    Next
    Send("{ENTER}")
    Sleep(250)
    Return True
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

Func _FindExportFormatDialogHandle()
    Local $list = WinList("[CLASS:#32770]")
    If @error Then Return ""
    If $list[0][0] < 1 Then Return ""

    Local $i
    For $i = 1 To $list[0][0]
        Local $h = $list[$i][1]
        If $h = "" Then ContinueLoop
        If Not WinExists($h) Then ContinueLoop

        If ControlGetHandle($h, "", "Edit1") <> "" And ControlGetHandle($h, "", "Button1") <> "" Then
            ContinueLoop
        EndIf

        Local $title = StringLower(StringStripWS(WinGetTitle($h), 3))
        If $title = "" Then ContinueLoop
        If StringInStr($title, "export") = 0 Then ContinueLoop

        Local $b1 = StringLower(StringStripWS(ControlGetText($h, "", "Button1"), 3))
        If $b1 = "" Then ContinueLoop
        If StringInStr($b1, "ok") = 0 And StringInStr($b1, "next") = 0 And StringInStr($b1, "finish") = 0 And StringInStr($b1, "export") = 0 Then
            ContinueLoop
        EndIf

        Return $h
    Next
    Return ""
EndFunc

Func _SelectExportFormatAndConfirm($hDlg, $format)
    If $hDlg = "" Then Return False
    WinActivate($hDlg)
    WinWaitActive($hDlg, "", 5)

    If $format = "CSV" Then
        Send("!c")
        Sleep(150)
        Send("c")
        Sleep(150)
    Else
        Send("!s")
        Sleep(150)
        Send("s")
        Sleep(150)
    EndIf

    _ClickPrimaryDialogButton($hDlg)
    Return True
EndFunc

Func _CompleteSaveAs($fullPath, $timeoutSec)
    Local $hDlg = _WaitForSaveAsDialogHandle($timeoutSec)
    If $hDlg = "" Then Return False

    ControlFocus($hDlg, "", "Edit1")
    ControlSetText($hDlg, "", "Edit1", $fullPath)
    _ClickSaveButton($hDlg)

    Local $t = TimerInit()
    Local $lastTry = TimerInit()
    While TimerDiff($t) < ($timeoutSec * 1000)
        Local $ovr = _FindOverwriteDialogHandle()
        If $ovr <> "" Then
            _ConfirmOverwriteDialog($ovr)
            WinWaitClose($ovr, "", 10)
            ContinueLoop
        EndIf
        If WinWaitClose($hDlg, "", 1) Then Return True
        If TimerDiff($lastTry) >= 1500 Then
            ControlFocus($hDlg, "", "Edit1")
            ControlSetText($hDlg, "", "Edit1", $fullPath)
            _ClickSaveButton($hDlg)
            $lastTry = TimerInit()
        EndIf
    WEnd
    Return False
EndFunc

Local $hMain = WaitForNesstar()

_Log("Opening study")
OpenStudy()
Sleep(8000)

Local $exportFormat = "SAV"
Local $dataExt = ".sav"

DirCreate($outputDir)

WinActivate($hMain)
WinWaitActive($hMain, "", 10)
_Stage("EXPORTING_ALL_DATASETS")
Send("^+e")

Local $saveHandled = False
Local $sawExportDialog = False
Local $tExport = TimerInit()
While TimerDiff($tExport) < (120 * 1000)
    Local $fmt = _FindExportFormatDialogHandle()
    If $fmt <> "" Then
        If Not $sawExportDialog Then
            _Stage("EXPORT_DIALOG_OPENED")
            $sawExportDialog = True
        EndIf
        _SelectExportFormatAndConfirm($fmt, $exportFormat)
        ContinueLoop
    EndIf

    Local $saveDlg = _FindSaveAsDialogHandle()
    If $saveDlg <> "" Then
        If Not $sawExportDialog Then
            _Stage("EXPORT_DIALOG_OPENED")
            $sawExportDialog = True
        EndIf
        _Stage("SAVING_DATASETS")
        Local $savePath = $outputDir & "\\datasets" & $dataExt
        $saveHandled = _CompleteSaveAs($savePath, 60)
        ExitLoop
    EndIf

    _DismissActiveModal()
    Sleep(300)
WEnd

If Not $saveHandled Then Exit 20

; Exit immediately after confirming the save dialog so the python watcher can take over
Exit 0
