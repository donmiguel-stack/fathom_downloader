#!/usr/bin/env python3
"""
Fathom Video Downloader
========================

Downloads every recording's video from a Fathom (fathom.video) account
using Fathom's official public API, for a chosen date range (defaults to
the last 2 years). Designed to run unattended: it can be stopped and
re-run at any time and will pick up where it left off.

SETUP (one-time)
-----------------
1. Log in to Fathom in your browser.
2. Go to Settings -> API Access, and generate an API key.
   (Docs: https://help.fathom.video/en/articles/8368641)
3. Install the one dependency this script needs:
       pip install requests
4. Set your API key as an environment variable (recommended so it never
   ends up in a saved file):
       export FATHOM_API_KEY="paste-your-key-here"      # macOS/Linux
       setx FATHOM_API_KEY "paste-your-key-here"         # Windows
   ...or pass it directly with --api-key when running the script.

RUNNING
-------
    python3 fathom_downloader.py

By default this downloads every recording from the last 2 years into a
new "fathom_videos" folder in the current directory. Useful options:

    --output-dir DIR     Where to save videos (default: ./fathom_videos)
    --years N            How many years back to go (default: 2)
    --since YYYY-MM-DD    Exact start date instead of --years
    --list-only          Just show how many meetings/videos would be
                          downloaded, without downloading anything
    --api-key KEY         Fathom API key (instead of env var)

The script keeps a small progress file (.fathom_download_state.json)
inside the output folder. If it's interrupted (closed laptop, network
drop, etc.) just run the same command again -- it will skip anything
already downloaded and resume the rest.

HOW IT WORKS
------------
Fathom's video download is a two-step, asynchronous affair: you POST to
ask for a recording's video, and Fathom hands back a download record
that becomes "completed" -- carrying a signed, time-limited URL -- once
the video is ready. Sometimes that's immediate; sometimes it isn't.

So this runs in two phases:

  1. Ask Fathom to prepare EVERY outstanding recording, up front. This
     matters: requesting one, waiting for it, then requesting the next
     serializes work that Fathom is perfectly happy to do in parallel.
  2. Sweep the whole list repeatedly, downloading whatever has become
     ready since the last pass, backing off between sweeps.

The sweep loop deliberately never exits while anything is still
outstanding. Under a process supervisor or Docker restart policy,
exiting early means an immediate restart from scratch -- an infinite
loop that looks like progress but never finishes anything.

NOTES ON FATHOM'S API LIMITS
-----------------------------
Fathom's API caps requests to the recording/download endpoints at
roughly 30 requests per 60 seconds (and this can drop further under
heavy load). This script paces its own requests to stay comfortably
under that limit, so a two-year library may take a while to fully
download -- that's expected and safe to leave running in the
background. The video transfers themselves go to signed storage URLs
that don't count against the API limit, so those run at full speed.
"""

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print(
        "This script needs the 'requests' package.\n"
        "Install it with:  pip install requests",
        file=sys.stderr,
    )
    sys.exit(1)


API_BASE = "https://api.fathom.ai/external/v1"
STATE_FILENAME = ".fathom_download_state.json"

# Streamed download chunk size.
CHUNK_SIZE = 1024 * 1024  # 1 MB
# Just used to pace the "still waiting" log message -- the sweep loop
# itself never gives up and exits while anything's still pending (see the
# comment above the cooldown logic in main() for why).
MAX_NO_PROGRESS_SWEEPS = 6
# Base pause between sweeps; backs off (up to the cap below) the longer a
# streak of sweeps goes without finding anything new, so we're not
# hammering the API pointlessly while Fathom is still rendering.
SWEEP_COOLDOWN_SECONDS = 45
MAX_SWEEP_COOLDOWN_SECONDS = 5 * 60


class RateLimiter:
    """Simple sliding-window limiter: at most `max_calls` calls per
    `period` seconds. Blocks (sleeps) as needed before each call."""

    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = deque()

    def wait(self):
        now = time.monotonic()
        while self.calls and now - self.calls[0] >= self.period:
            self.calls.popleft()
        if len(self.calls) >= self.max_calls:
            sleep_for = self.period - (now - self.calls[0]) + 0.05
            if sleep_for > 0:
                time.sleep(sleep_for)
            return self.wait()
        self.calls.append(time.monotonic())


class FathomClient:
    """Thin wrapper around the Fathom public API with built-in rate
    limiting and retry-on-429/5xx behavior."""

    def __init__(self, api_key):
        self.session = requests.Session()
        self.session.headers.update({"X-Api-Key": api_key})
        # General endpoints (e.g. listing meetings without transcript/
        # summary) share a 60-requests/60s global budget. We stay a
        # little under that to leave headroom.
        self.general_limiter = RateLimiter(max_calls=50, period=60)
        # /recordings endpoints (download request + status polling) are
        # capped more tightly (~30/60s, sometimes less under load), so
        # we're conservative here.
        self.recordings_limiter = RateLimiter(max_calls=20, period=60)

    def _request(self, method, path, limiter, **kwargs):
        url = f"{API_BASE}{path}"
        max_attempts = 8
        last_exc = None
        for attempt in range(1, max_attempts + 1):
            limiter.wait()
            try:
                resp = self.session.request(method, url, timeout=60, **kwargs)
            except requests.exceptions.RequestException as e:
                last_exc = e
                delay = min(60, 2 ** attempt)
                print(f"  Network hiccup ({e.__class__.__name__}), retrying in {delay:.0f}s...")
                time.sleep(delay)
                continue
            last_exc = None
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else min(60, 2 ** attempt)
                print(f"  Rate limited, waiting {delay:.0f}s...")
                time.sleep(delay)
                continue
            if resp.status_code >= 500:
                delay = min(60, 2 ** attempt)
                print(f"  Server error {resp.status_code}, retrying in {delay:.0f}s...")
                time.sleep(delay)
                continue
            if resp.status_code == 401:
                print(
                    "Fathom rejected the API key (401 Unauthorized). "
                    "Double-check FATHOM_API_KEY / --api-key.",
                    file=sys.stderr,
                )
                sys.exit(1)
            return resp
        if last_exc is not None:
            raise last_exc
        return resp  # last attempt's response, even if it's an error

    def list_meetings(self, created_after_iso, cursor=None):
        params = {"created_after": created_after_iso}
        if cursor:
            params["cursor"] = cursor
        resp = self._request("GET", "/meetings", self.general_limiter, params=params)
        resp.raise_for_status()
        return resp.json()

    def request_download(self, recording_id):
        resp = self._request(
            "POST",
            f"/recordings/{recording_id}/download",
            self.recordings_limiter,
            json={},
        )
        resp.raise_for_status()
        return resp.json()

    def get_download_status(self, recording_id, download_id):
        resp = self._request(
            "GET",
            f"/recordings/{recording_id}/downloads/{download_id}",
            self.recordings_limiter,
        )
        resp.raise_for_status()
        return resp.json()

    def download_file(self, file_url, dest_path):
        # The signed file_url is pre-authenticated; no rate limiting
        # needed here, it's not a Fathom API endpoint call.
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
        with self.session.get(file_url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length") or 0)
            written = 0
            next_report = 100 * 1024 * 1024  # log every ~100 MB
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    f.write(chunk)
                    written += len(chunk)
                    # Some of these run well over a gigabyte; without this a
                    # long download is indistinguishable from a hang.
                    if written >= next_report:
                        if total:
                            print(f"    ...{written / 1e6:.0f} MB of {total / 1e6:.0f} MB")
                        else:
                            print(f"    ...{written / 1e6:.0f} MB")
                        next_report += 100 * 1024 * 1024
        tmp_path.rename(dest_path)


def extract_file_url(payload):
    """Pull the downloadable video URL out of a Fathom download payload.

    Fathom nests it as {"video": {"url": ...}}; we also accept a couple of
    plausible flat spellings so a future API tweak doesn't silently break
    us back into the "waits forever on already-finished videos" bug.
    """
    if not isinstance(payload, dict):
        return None
    video = payload.get("video")
    if isinstance(video, dict) and video.get("url"):
        return video["url"]
    return payload.get("file_url") or payload.get("url")


def sanitize_filename(name, max_len=120):
    name = unicodedata.normalize("NFKD", name)
    name = re.sub(r'[\\/:*?"<>|]', "-", name)
    name = re.sub(r"\s+", " ", name).strip()
    if not name:
        name = "untitled"
    return name[:max_len]


def load_state(state_path):
    if state_path.exists():
        try:
            with open(state_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            print(f"Warning: could not read {state_path}, starting fresh state.")
    return {}


def save_state(state_path, state):
    tmp = state_path.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    tmp.replace(state_path)


def fetch_all_meetings(client, since_dt):
    since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    meetings = []
    cursor = None
    while True:
        page = client.list_meetings(since_iso, cursor=cursor)
        items = page.get("items", [])
        meetings.extend(items)
        cursor = page.get("next_cursor")
        print(f"  Fetched {len(meetings)} meetings so far...")
        if not cursor or not items:
            break
    return meetings


def build_filename(meeting):
    created_at = meeting.get("recording_start_time") or meeting.get("created_at") or ""
    date_part = created_at[:10] if created_at else "unknown-date"
    title = sanitize_filename(meeting.get("title") or f"meeting-{meeting.get('recording_id')}")
    recording_id = meeting.get("recording_id")
    return f"{date_part} - {title} ({recording_id}).mp4"


def _get_entry(state, meeting):
    recording_id = str(meeting["recording_id"])
    entry = state.setdefault(recording_id, {})
    entry["filename"] = build_filename(meeting)
    return recording_id, entry


def ensure_download_requested(client, meeting, state, state_path):
    """Make sure Fathom has been asked to start rendering this recording.
    Does NOT wait around for it to finish -- just kicks it off (or does
    nothing if we already asked, or it's already done)."""
    recording_id, entry = _get_entry(state, meeting)
    if entry.get("status") == "completed":
        return
    if entry.get("permanent_failure"):
        return  # Fathom flatly refuses this one; no point asking again every sweep
    if entry.get("download_id") or entry.get("file_url"):
        return  # already requested (or already have the link) from a prior sweep/run
    try:
        result = client.request_download(recording_id)
    except requests.exceptions.RequestException as e:
        # A 4xx that isn't rate-limiting means Fathom won't ever serve this
        # recording (e.g. 422 on a recording with no downloadable video), so
        # stop asking -- otherwise it spams the log on every single sweep.
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        permanent = code is not None and 400 <= code < 500 and code != 429
        if permanent:
            print(f"  Fathom won't provide a video for {recording_id} (HTTP {code}) -- skipping it from now on")
            entry["permanent_failure"] = True
        else:
            print(f"  Failed to request download for {recording_id}: {e}")
        entry["status"] = "failed"
        entry["error"] = str(e)
        save_state(state_path, state)
        return
    entry["download_id"] = result.get("download_id")
    entry["status"] = result.get("status", "pending")
    # Fathom often hands back the finished video right here in the POST
    # response, already rendered -- grab it rather than making a pointless
    # follow-up status call.
    url = extract_file_url(result)
    if url:
        entry["file_url"] = url
    save_state(state_path, state)


def check_and_maybe_download(client, meeting, output_dir, state, state_path):
    """Check this recording's render status once. If it's ready, download
    it. If not, just report back -- no waiting/polling loop here, the
    caller re-checks on the next sweep instead."""
    recording_id, entry = _get_entry(state, meeting)
    filename = entry["filename"]
    dest_path = output_dir / filename

    if entry.get("status") == "completed" and dest_path.exists():
        return "skipped"

    download_id = entry.get("download_id")
    if not download_id:
        return "pending"  # not yet requested (will be picked up next sweep)

    file_url = entry.get("file_url")
    if not file_url:
        try:
            status = client.get_download_status(recording_id, download_id)
        except requests.exceptions.RequestException as e:
            print(f"  Status check failed for {recording_id}: {e}")
            return "pending"
        status_value = status.get("status")
        ready_url = extract_file_url(status)
        if status_value == "completed" and ready_url:
            file_url = ready_url
            entry["file_url"] = file_url
            save_state(state_path, state)
        elif status_value in ("expired", "failed", "error", "canceled", "cancelled"):
            # The download request itself died (e.g. it expired before
            # Fathom finished rendering it, likely because we fired off a
            # big batch of requests at once and this one's render didn't
            # finish inside the window). Clear it so the next line in the
            # sweep loop asks Fathom for a fresh one -- otherwise we'd poll
            # a dead request forever and never make progress.
            print(f"  Download {status_value} for {recording_id} -- requesting a fresh one")
            entry.pop("download_id", None)
            entry.pop("file_url", None)
            entry["status"] = "pending"
            save_state(state_path, state)
            return "pending"
        else:
            return "pending"  # still rendering, try again next sweep

    print(f"  Downloading: {filename}")
    try:
        client.download_file(file_url, dest_path)
    except requests.exceptions.RequestException as e:
        print(f"  Download failed (link may have expired) for {recording_id}: {e}")
        # Drop the stale file_url/download_id so the next sweep requests fresh ones.
        entry.pop("file_url", None)
        entry.pop("download_id", None)
        entry["status"] = "pending"
        save_state(state_path, state)
        return "failed"

    entry["status"] = "completed"
    save_state(state_path, state)
    return "downloaded"


def main():
    parser = argparse.ArgumentParser(description="Download all your Fathom recording videos.")
    parser.add_argument("--api-key", default=os.environ.get("FATHOM_API_KEY"),
                         help="Fathom API key (defaults to FATHOM_API_KEY env var)")
    parser.add_argument("--output-dir", default="fathom_videos",
                         help="Folder to save videos into (default: ./fathom_videos)")
    parser.add_argument("--years", type=float, default=2,
                         help="How many years back to download (default: 2)")
    parser.add_argument("--since", default=None,
                         help="Exact start date YYYY-MM-DD, overrides --years")
    parser.add_argument("--list-only", action="store_true",
                         help="Only list how many meetings would be downloaded")
    args = parser.parse_args()

    if not args.api_key:
        print(
            "No API key found. Set FATHOM_API_KEY or pass --api-key.\n"
            "Get a key from Fathom: Settings -> API Access.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        since_dt = datetime.now(timezone.utc) - timedelta(days=int(args.years * 365.25))

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / STATE_FILENAME
    state = load_state(state_path)

    client = FathomClient(args.api_key)

    print(f"Fetching meetings recorded since {since_dt.date().isoformat()}...")
    meetings = fetch_all_meetings(client, since_dt)
    print(f"Found {len(meetings)} meetings.\n")

    if args.list_only:
        already = sum(1 for m in meetings if state.get(str(m["recording_id"]), {}).get("status") == "completed")
        print(f"{already} already downloaded, {len(meetings) - already} remaining.")
        return

    def not_completed(m):
        return state.get(str(m["recording_id"]), {}).get("status") != "completed"

    # Phase 1: ask Fathom to start rendering EVERY remaining recording up
    # front, as fast as the rate limit allows. This is what actually lets
    # Fathom work on many videos in parallel on their end, instead of us
    # accidentally serializing everything by waiting on one at a time.
    to_request = [m for m in meetings if not_completed(m)]
    if to_request:
        print(f"Requesting video generation for {len(to_request)} meeting(s)...")
        for i, meeting in enumerate(to_request, 1):
            ensure_download_requested(client, meeting, state, state_path)
            if i % 20 == 0 or i == len(to_request):
                print(f"  Requested {i}/{len(to_request)}...")

    # Phase 2: sweep through everyone repeatedly, checking status once per
    # sweep and downloading whatever's ready. Cheap and fast per sweep
    # since each check is a single quick API call, not a wait loop.
    total_counts = {"downloaded": 0, "skipped": 0, "failed": 0, "pending": 0}
    sweep = 0
    no_progress_sweeps = 0
    while True:
        pending_meetings = [m for m in meetings if not_completed(m)]
        if not pending_meetings:
            break
        sweep += 1
        print(f"\n--- Sweep {sweep}: {len(pending_meetings)} meeting(s) remaining ---\n")
        sweep_counts = {"downloaded": 0, "skipped": 0, "failed": 0, "pending": 0}
        for i, meeting in enumerate(pending_meetings, 1):
            title = meeting.get("title", "(untitled)")
            # No-op for anything that already has a download_id/file_url or
            # is completed -- only actually fires a new request for
            # recordings that were just cleared (e.g. an expired download).
            ensure_download_requested(client, meeting, state, state_path)
            result = check_and_maybe_download(client, meeting, output_dir, state, state_path)
            if result != "pending":
                print(f"[{i}/{len(pending_meetings)}] {title}: {result}")
            sweep_counts[result] = sweep_counts.get(result, 0) + 1
            total_counts[result] = total_counts.get(result, 0) + 1

        print(
            f"\nSweep {sweep} done: {sweep_counts['downloaded']} downloaded, "
            f"{sweep_counts['pending']} still rendering, {sweep_counts['failed']} failed."
        )
        if sweep_counts["downloaded"] == 0:
            no_progress_sweeps += 1
        else:
            no_progress_sweeps = 0

        if no_progress_sweeps > 0 and no_progress_sweeps % MAX_NO_PROGRESS_SWEEPS == 0:
            print(
                f"\nNo new videos finished rendering across the last {no_progress_sweeps} "
                "sweeps -- still waiting on Fathom to finish rendering the rest. "
                "This is normal for long recordings; staying up and continuing to check."
            )

        # Back off the more consecutive sweeps come back empty, so we're not
        # hammering the API once it's clear things are rendering slowly --
        # but NEVER exit while anything's still pending. Exiting here would
        # let `restart: unless-stopped` immediately restart the whole run
        # from scratch, which is exactly the loop we don't want.
        cooldown = min(
            SWEEP_COOLDOWN_SECONDS * (2 ** min(no_progress_sweeps, 5)),
            MAX_SWEEP_COOLDOWN_SECONDS,
        )
        print(f"Cooling down {cooldown}s before the next sweep...")
        time.sleep(cooldown)

    remaining = sum(1 for m in meetings if not_completed(m))
    print("\nDone.")
    print(f"  Downloaded this run:  {total_counts['downloaded']}")
    print(f"  Still not downloaded: {remaining} (re-run the script to keep retrying)")
    print(f"\nVideos saved to: {output_dir}")


if __name__ == "__main__":
    main()
