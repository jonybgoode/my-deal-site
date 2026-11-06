#!/usr/bin/env python3
# NOTE: If .github/workflows/update-deals.yml is active, you no longer need
# to run this script locally for daily updates. The cloud workflow handles
# deployment automatically.
"""
deploy.py — publish the deal catalog to GitHub Pages.

Flow:
    1. (optional, --fetch) run the pipeline first: python deal_finder.py --once
    2. copy deals.html -> index.html          (GitHub Pages serves index.html at /)
       copy coupons.html -> coupons.html      (coupon-code deals page, if present)
    3. git init -b gh-pages                   (or reuse the existing repo on gh-pages)
    4. git add index.html coupons.html && git commit -m "Auto-update deals: <timestamp>"
    5. git push -f origin gh-pages

If the push fails (e.g. no remote configured), a step-by-step setup guide
is printed. Use --no-push to test everything locally up to the commit.

Usage:
    python deploy.py                # copy + commit + force-push gh-pages
    python deploy.py --fetch        # refresh deals first (runs the pipeline once)
    python deploy.py --no-push      # local test: copy + commit, skip the push
"""

import argparse
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

PROJECT_DIR = Path(__file__).resolve().parent
DEALS_HTML = PROJECT_DIR / "deals.html"
INDEX_HTML = PROJECT_DIR / "index.html"
COUPONS_HTML = PROJECT_DIR / "coupons.html"

SETUP_HELP = """\
------------------------------------------------------------------
git push failed — here is how to finish the one-time setup:
------------------------------------------------------------------
1. Create a new PUBLIC repository on GitHub: https://github.com/new
   (skip README / .gitignore / license — deploy.py manages the content).
2. Connect this folder to it:
       git remote add origin https://github.com/<your-username>/<your-repo>.git
   If 'origin' already exists but points at the wrong repo, fix it with:
       git remote set-url origin https://github.com/<your-username>/<your-repo>.git
3. Run the deploy again:
       python deploy.py
   The first push may ask you to log in — Git Credential Manager
   (Windows) handles this automatically; on Linux/Mac use a Personal
   Access Token with 'repo' scope as the password.
4. In the GitHub repo: Settings -> Pages -> Branch: 'gh-pages',
   folder '/ (root)' -> Save.
5. Your deals page will be live at:
       https://<your-username>.github.io/<your-repo>/
"""


def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=PROJECT_DIR, capture_output=True, text=True
    )


def die_on_git_error(result: subprocess.CompletedProcess, what: str) -> None:
    print(f"[deploy] ERROR: `git {what}` failed:", file=sys.stderr)
    if result.stdout.strip():
        print(result.stdout.strip(), file=sys.stderr)
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)
    sys.exit(1)


def ensure_gh_pages_branch() -> None:
    """Fresh dir -> git init -b gh-pages; existing repo -> switch to gh-pages."""
    if not (PROJECT_DIR / ".git").exists():
        result = git("init", "-b", "gh-pages")
        if result.returncode != 0:
            result = git("init")
            if result.returncode != 0:
                die_on_git_error(result, "init")
            symbolic = git("symbolic-ref", "HEAD", "refs/heads/gh-pages")
            if symbolic.returncode != 0:
                die_on_git_error(symbolic, "symbolic-ref")
        print("[deploy] Initialized a fresh git repo on branch gh-pages")
        return

    branch = git("symbolic-ref", "--short", "HEAD")
    if branch.returncode != 0:
        die_on_git_error(branch, "symbolic-ref")
    current = branch.stdout.strip()
    if current == "gh-pages":
        print("[deploy] Already on branch gh-pages")
        return

    checkout = git("checkout", "gh-pages")
    if checkout.returncode != 0:
        orphan = git("checkout", "--orphan", "gh-pages")
        if orphan.returncode != 0:
            die_on_git_error(orphan, "checkout --orphan gh-pages")
        git("rm", "-rf", "--cached", ".")
        print("[deploy] Switched to a fresh orphan branch gh-pages")
    else:
        print("[deploy] Switched to existing branch gh-pages")


def ensure_git_identity() -> None:
    """Set a repo-local git identity if none is configured (first-run fix)."""
    email = git("config", "user.email")
    if email.returncode == 0 and email.stdout.strip():
        return
    git("config", "user.name", "Deal Finder Deploy")
    git("config", "user.email", "deploy@deal-finder.local")
    print(
        "[deploy] No git identity found — set a repo-local one "
        "(Deal Finder Deploy <deploy@deal-finder.local>). "
        "Change it with: git config user.email you@example.com"
    )


def pages_url_from_remote(remote_url: str) -> Optional[str]:
    match = re.search(
        r"github\.com[/:]([^/]+)/([^/]+?)(?:\.git)?/?$", remote_url.strip()
    )
    if not match:
        return None
    user, repo = match.group(1), match.group(2)
    return f"https://{user}.github.io/{repo}/"


def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Publish deals.html (+ coupons.html) to GitHub Pages"
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="run `python deal_finder.py --once` before publishing",
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="do everything except the git push (local test)",
    )
    args = parser.parse_args()

    if args.fetch:
        print("[deploy] Running the pipeline (--fetch) ...")
        fetched = subprocess.run(
            [sys.executable, "deal_finder.py", "--once"], cwd=PROJECT_DIR
        )
        if fetched.returncode != 0:
            print("[deploy] ERROR: pipeline run failed; aborting deploy", file=sys.stderr)
            return 1

    if not DEALS_HTML.exists():
        print(
            "[deploy] ERROR: deals.html not found — run `python deal_finder.py --once` "
            "first (or use --fetch), then retry.",
            file=sys.stderr,
        )
        return 1

    shutil.copyfile(DEALS_HTML, INDEX_HTML)
    print(
        f"[deploy] Copied deals.html -> index.html "
        f"({INDEX_HTML.stat().st_size:,} bytes)"
    )

    # The coupons page rides along when it exists (rendered by every
    # pipeline run since the promo-code split). It keeps its own name.
    to_commit = ["index.html"]
    if COUPONS_HTML.exists():
        print(
            f"[deploy] Publishing coupons.html "
            f"({COUPONS_HTML.stat().st_size:,} bytes)"
        )
        to_commit.append("coupons.html")
    else:
        print("[deploy] coupons.html not found — deploying index.html only")

    ensure_gh_pages_branch()
    ensure_git_identity()

    added = git("add", *to_commit)
    if added.returncode != 0:
        die_on_git_error(added, f"add {' '.join(to_commit)}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    committed = git("commit", "-m", f"Auto-update deals: {timestamp}")
    if committed.returncode != 0:
        commit_out = committed.stdout + committed.stderr
        if "nothing to commit" in commit_out or "nothing added to commit" in commit_out:
            print("[deploy] No changes since the last deploy commit")
        elif "Please tell me who you are" in (committed.stdout + committed.stderr):
            print(
                "[deploy] ERROR: git identity is not configured. Run:\n"
                "  git config --global user.name  \"Your Name\"\n"
                "  git config --global user.email \"you@example.com\"\n"
                "then retry.",
                file=sys.stderr,
            )
            return 1
        else:
            die_on_git_error(committed, "commit")
    else:
        print(f"[deploy] Committed index.html (Auto-update deals: {timestamp})")

    if args.no_push:
        print("[deploy] --no-push set: skipping the push. Everything else succeeded.")
        return 0

    remote = git("remote", "get-url", "origin")
    if remote.returncode != 0:
        print("[deploy] ERROR: no git remote named 'origin' is configured.\n",
              file=sys.stderr)
        print(SETUP_HELP)
        return 1

    pushed = git("push", "-f", "origin", "gh-pages")
    if pushed.returncode != 0:
        print("[deploy] ERROR: `git push -f origin gh-pages` failed.\n", file=sys.stderr)
        if pushed.stderr.strip():
            print(pushed.stderr.strip(), file=sys.stderr)
        print(file=sys.stderr)
        print(SETUP_HELP)
        return 1

    remote_url = remote.stdout.strip()
    url = pages_url_from_remote(remote_url)
    print(f"[deploy] Pushed gh-pages to {remote_url}")
    if url:
        print(f"[deploy] Live URL (once Pages is enabled): {url}")
    else:
        print("[deploy] Could not derive the Pages URL from the remote — see README")
    return 0


if __name__ == "__main__":
    sys.exit(main())
