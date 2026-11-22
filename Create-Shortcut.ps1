# Creates a desktop shortcut "Deal Finder Push" that runs Push-To-GitHub.bat
# (cmd window stays open via the bat's own pause). Re-run any time to
# recreate the icon.
#
# If the execution policy blocks this script, run it via:
#   powershell -ExecutionPolicy Bypass -File .\Create-Shortcut.ps1

$ErrorActionPreference = "Stop"

$projectRoot = "C:\Users\jonjo\Desktop\OpenCode Projects\deal-finder"
$shortcutPath = Join-Path $env:USERPROFILE "Desktop\Deal Finder Push.lnk"

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = "cmd.exe"
$shortcut.Arguments = "/c `"$projectRoot\Push-To-GitHub.bat`""
$shortcut.WorkingDirectory = $projectRoot
$shortcut.IconLocation = "$env:SystemRoot\System32\shell32.dll,21"
$shortcut.Description = "Push Deal Finder updates to GitHub"
$shortcut.Save()

Write-Host "Shortcut created: $shortcutPath"
