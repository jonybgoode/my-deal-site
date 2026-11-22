@echo off
rem One-click: push the deal-finder source (and the GitHub Actions workflow)
rem to https://github.com/jonybgoode/my-deal-site.git on the main branch.
rem Idempotent: safe to re-run; only pushes what changed.
cd /d "C:\Users\jonjo\Desktop\OpenCode Projects\deal-finder"
if errorlevel 1 (
  echo ERROR: project folder not found.
  pause >nul
  exit /b 1
)

rem --- remote: add origin only if it isn't configured yet ---
git remote | findstr /x "origin" >nul
if errorlevel 1 (
  echo Adding remote origin...
  git remote add origin https://github.com/jonybgoode/my-deal-site.git
)

rem --- branch: create orphan main on first run, switch to it afterwards ---
git show-ref --verify --quiet refs/heads/main
if errorlevel 1 (
  echo Creating orphan main branch...
  git switch --orphan main
) else (
  git switch main
)

rem --- stage, commit, push ---
echo Staging files...
git add .github deal_finder.py deploy.py templates requirements.txt .env.example README.md .gitignore affiliate_feed_sample.json

echo Committing...
git commit -m "Add deal-finder pipeline + GitHub Actions auto-update workflow"
if errorlevel 1 echo Nothing new to commit - pushing existing state.

echo Pushing to origin main...
git push -u origin main --force

echo Done! Press any key to exit.
pause >nul
