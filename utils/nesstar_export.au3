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
Local $schema = ""
If $CmdLine[0] >= 4 Then
    $schema = $CmdLine[4]
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

Func _WaitForSaveDialog($timeoutSec)
    Local $t = TimerInit()
    While TimerDiff($t) < ($timeoutSec * 1000)
        Local $dlg = _FindAnyDialogHandle()
        If $dlg <> "" Then Return $dlg
        _DismissActiveModal()
        Sleep(200)
    WEnd
    Return ""
EndFunc

Func _FindWizardDialogHandle()
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
        Local $b1 = ControlGetHandle($h, "", "Button1")
        Local $b2 = ControlGetHandle($h, "", "Button2")
        If $b1 = "" And $b2 = "" Then ContinueLoop
        Local $t1 = StringLower(StringStripWS(ControlGetText($h, "", "Button1"), 3))
        Local $t2 = StringLower(StringStripWS(ControlGetText($h, "", "Button2"), 3))
        If StringInStr($t1, "next") > 0 Or StringInStr($t2, "next") > 0 Or StringInStr($t1, "finish") > 0 Or StringInStr($t2, "finish") > 0 Then
            Return $h
        EndIf
    Next
    Return ""
EndFunc

Func _WaitForSaveDialogOrWizard($timeoutSec)
    Local $t = TimerInit()
    While TimerDiff($t) < ($timeoutSec * 1000)
        Local $dlg = _FindAnyDialogHandle()
        If $dlg <> "" Then Return $dlg

        Local $wiz = _FindWizardDialogHandle()
        If $wiz <> "" Then
            _Stage("CONFIRMING_EXPORT_OPTIONS")
            WinActivate($wiz)
            WinWaitActive($wiz, "", 5)
            Local $k
            For $k = 1 To 12
                Send("{ENTER}")
                Sleep(800)
                Local $d2 = _FindAnyDialogHandle()
                If $d2 <> "" Then Return $d2
                If Not WinExists($wiz) Then ExitLoop
            Next
        EndIf

        _DismissActiveModal()
        Sleep(200)
    WEnd
    Return ""
EndFunc

Func _NormalizeExportFormat($s)
    Local $t = StringUpper(StringStripWS($s, 3))
    If $t = "SAV" Or $t = "SPSS" Then Return "SAV"
    Return "CSV"
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

Func _WaitForDataFilesStable($outputDir, $pattern, $minBytes, $timeoutSec)
    Local $t = TimerInit()
    Local $stableSince = TimerInit()
    Local $lastSig = ""

    While TimerDiff($t) < ($timeoutSec * 1000)
        Local $h = FileFindFirstFile($outputDir & "\\" & $pattern)
        Local $count = 0
        Local $sum = 0
        Local $min = -1
        Local $max = 0
        If $h <> -1 Then
            While 1
                Local $f = FileFindNextFile($h)
                If @error Then ExitLoop
                $count += 1
                Local $p = $outputDir & "\\" & $f
                Local $sz = FileGetSize($p)
                $sum += $sz
                If $min = -1 Or $sz < $min Then $min = $sz
                If $sz > $max Then $max = $sz
            WEnd
            FileClose($h)
        EndIf

        Local $sig = $count & "|" & $sum & "|" & $min & "|" & $max
        If $sig <> $lastSig Then
            $lastSig = $sig
            $stableSince = TimerInit()
        EndIf

        If $count > 0 And $min >= $minBytes Then
            If TimerDiff($stableSince) >= 5000 Then
                Return True
            EndIf
        EndIf

        Sleep(750)
    WEnd
    Return False
EndFunc

Func _HandleWizardOrDialogAndConfirm($timeoutSec, $outputDir, $defaultFileName)
    Local $t = TimerInit()
    While TimerDiff($t) < ($timeoutSec * 1000)
        Local $dlg = _FindAnyDialogHandle()
        If $dlg <> "" Then
            WinActivate($dlg)
            WinWaitActive($dlg, "", 10)
            Local $path = $outputDir
            If $defaultFileName <> "" Then
                $path = $outputDir & "\\" & $defaultFileName
            EndIf
            ControlSetText($dlg, "", "Edit1", $path)
            ControlSend($dlg, "", "Edit1", "{ENTER}")
            Return True
        EndIf

        Local $wiz = _FindWizardDialogHandle()
        If $wiz <> "" Then
            WinActivate($wiz)
            WinWaitActive($wiz, "", 10)
            If ControlGetHandle($wiz, "", "Edit1") <> "" Then
                ControlSetText($wiz, "", "Edit1", $outputDir)
            EndIf
            Local $k
            For $k = 1 To 14
                Send("{ENTER}")
                Sleep(900)
                If Not WinExists($wiz) Then ExitLoop
                If _FindAnyDialogHandle() <> "" Then ExitLoop
            Next
            Return True
        EndIf

        _DismissActiveModal()
        Sleep(200)
    WEnd
    Return False
EndFunc

Func _WaitForFileMinSize($path, $minBytes, $timeoutSec)
    Local $t = TimerInit()
    While TimerDiff($t) < ($timeoutSec * 1000)
        If FileExists($path) Then
            Local $sz = FileGetSize($path)
            If $sz >= $minBytes Then Return True
        EndIf
        Sleep(500)
    WEnd
    Return False
EndFunc

If $titleHint = "DRY_RUN" Then
    DirCreate($outputDir)
    _Stage("EXPORT_DIALOG_OPENED")
    Local $f1 = FileOpen($outputDir & "\LEVEL-01.sav", 18)
    If $f1 <> -1 Then
        FileWrite($f1, StringRepeat("A", 2 * 1024 * 1024))
        FileClose($f1)
    EndIf
    Local $f2 = FileOpen($outputDir & "\LEVEL-02.sav", 18)
    If $f2 <> -1 Then
        FileWrite($f2, StringRepeat("B", 2 * 1024 * 1024))
        FileClose($f2)
    EndIf
    Exit 0
EndIf

Local $hMain = WaitForNesstar()

_Log("Opening study")
OpenStudy()
Sleep(8000)

Local $exportFormat = "SAV"
Local $dataExt = ".sav"
Local $dataPattern = "*.sav"

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

    Local $wiz = _FindWizardDialogHandle()
    If $wiz <> "" Then
        If Not $sawExportDialog Then
            _Stage("EXPORT_DIALOG_OPENED")
            $sawExportDialog = True
        EndIf
        WinActivate($wiz)
        WinWaitActive($wiz, "", 5)
        Local $k
        For $k = 1 To 12
            Send("{ENTER}")
            Sleep(800)
            If _FindAnyDialogHandle() <> "" Then ExitLoop
            If Not WinExists($wiz) Then ExitLoop
        Next
        ContinueLoop
    EndIf

    _DismissActiveModal()
    Sleep(300)
WEnd

If Not $saveHandled Then Exit 20

If Not _WaitForDataFilesStable($outputDir, $dataPattern, 1024, 900) Then Exit 21

WinActivate($hMain)
WinWaitActive($hMain, "", 5)
Send("!f")
Send("x")

Exit 0
