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

    --output-dir DIR         Where to save videos (default: ./fathom_videos)
    --years N                How many years back to go (default: 2)
    --since YYYY-MM-DD       Exact start date instead of --years
    --list-only              Just show how many meetings/videos would be
                              downloaded, without downloading anything
    --api-key KEY            Fathom API key (instead of env var)
    --max-rate-kbps N        Throttle downloads to N KB/s (0 = unlimited)
    --delay-between-files N  Pause N seconds between videos
    --max-runtime-hours N    Stop cleanly after N hours (0 = no limit)
    --max-idle-sweeps N      Give up after N sweeps with zero downloads

The script keeps a small progress file (.fathom_download_state.json)
inside the output folder. If it's interrupted (closed laptop, network
drop, etc.) just run the same command again -- it will skip anything
already downloaded and resume the rest, including picking up part-way
through a half-transferred file.

HOW IT WORKS
------------
Fathom's video download is a two-step, asynchronous affair: you POST to
ask for a recording's video, and Fathom hands back a download record
that becomes "completed" -- carrying a signed, time-limited URL -- once
the video is ready. Sometimes that's immediate; sometimes it takes
hours, and across a large library some recordings stay unrendered far
longer than you'd like.

So this runs in two phases:

  1. Ask Fathom to prepare EVERY outstanding recording, up front. This
     matters: requesting one, waiting for it, then requesting the next
     serializes work that Fathom is perfectly happy to do in parallel.
  2. Sweep the list repeatedly, downloading whatever became ready since
     the last pass, backing off between sweeps, and stopping once
     several sweeps in a row have produced nothing.

That stop condition matters as much as the work does. Fathom's
rendering does not resolve on a timescale of minutes, so sweeping a
list of still-rendering recordings indefinitely burns bandwidth and API
quota to no purpose. This exits cleanly instead, reporting what's still
outstanding so you know what a later run will pick up.

  !! IF YOU RUN THIS UNDER DOCKER OR A PROCESS SUPERVISOR: make sure a
  !! clean exit does NOT trigger a restart. Under `restart:
  !! unless-stopped`, exiting on the idle-sweep ceiling restarts the
  !! whole run from scratch, forever -- an infinite loop that looks
  !! like healthy activity in the logs while finishing nothing. Use
  !! `restart: on-failure` (see the bundled docker-compose.yml).

Signed download URLs are time-limited, so this fetches a fresh one
immediately before each download rather than trusting one collected at
the start of a long run.

NOTES ON FATHOM'S API LIMITS
-----------------------------
Fathom's API caps requests to the recording/download endpoints at
roughly 30 requests per 60 seconds (and this can drop further under
heavy load). This script paces its own requests to stay comfortably
under that limit, so a two-year library may take a while to fully
download -- that's expected and safe to leave running in the
background.

The video transfers themselves go to signed storage URLs that don't
count against the API limit, so those otherwise run at full speed --
which on a shared or metered connection (satellite, tethered, or just a
household where other people are trying to use the internet) can flatten
the link entirely. Use --max-rate-kbps to leave headroom for everyone
else.
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

# Streamed download chunk size. Deliberately under a megabyte: with
# throttling on, large chunks make the rate limiter lumpy (a burst, then
# a long sleep) rather than a smooth trickle.
CHUNK_SIZE = 512 * 1024

# Connect and read timeouts, in seconds. The read timeout is the gap
# between bytes, not the total transfer time, so large files are fine.
# Keep it short: on a saturated link a dead connection otherwise ties up
# the socket doing nothing before it finally gives up.
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 30

# Pause between sweeps, growing with each fruitless one.
SWEEP_COOLDOWN_SECONDS = 60
MAX_SWEEP_COOLDOWN_SECONDS = 15 * 60


class IncompleteDownload(Exception):
    """A transfer finished but isn't the size Fathom said it would be."""


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


class BandwidthLimiter:
    """Paces downloads to roughly `max_bytes_per_sec`.

    Downloading flat-out is antisocial on a shared or metered link -- it
    can make a household's connection unusable for as long as it runs.
    After each chunk this works out how long the bytes so far *should*
    have taken and sleeps off the difference.
    """

    def __init__(self, max_bytes_per_sec):
        self.max_bytes_per_sec = max_bytes_per_sec
        self.reset()

    def reset(self):
        self.started = time.monotonic()
        self.bytes_seen = 0

    def consume(self, n):
        if not self.max_bytes_per_sec:
            return
        self.bytes_seen += n
        expected_elapsed = self.bytes_seen / self.max_bytes_per_sec
        actual_elapsed = time.monotonic() - self.started
        if expected_elapsed > actual_elapsed:
            time.sleep(expected_elapsed - actual_elapsed)


class FathomClient:
    """Thin wrapper around the Fathom public API with built-in rate
    limiting and retry-on-429/5xx behavior."""

    def __init__(self, api_key, bandwidth_limiter=None):
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
        self.bandwidth = bandwidth_limiter or BandwidthLimiter(0)

    def _request(self, method, path, limiter, **kwargs):
        url = f"{API_BASE}{path}"
        max_attempts = 8
        last_exc = None
        for attempt in range(1, max_attempts + 1):
            limiter.wait()
            try:
                resp = self.session.request(
                    method, url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), **kwargs
                )
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

    def list_meetings(self, created_after_iso, cursor=None, include_text=False):
        params = {"created_after": created_after_iso}
        if cursor:
            params["cursor"] = cursor
        if include_text:
            # Transcripts and summaries ride along with the meeting list,
            # so the whole library's text costs one paginated pass rather
            # than two API calls per recording.
            params["include_transcript"] = "true"
            params["include_summary"] = "true"
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

    def download_file(self, file_url, dest_path, expected_size=None):
        """Stream a video to disk, resuming a partial file where possible.

        Writes to a .part file and only renames on success, so an
        interrupted transfer can never masquerade as a finished video.
        """
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")

        resume_from = 0
        if tmp_path.exists():
            existing = tmp_path.stat().st_size
            if expected_size and existing == expected_size:
                # It transferred fully last time; we just never renamed it.
                tmp_path.rename(dest_path)
                return
            if expected_size and existing > expected_size:
                # Nonsense state -- start over rather than reason about it.
                tmp_path.unlink()
            elif existing > 0:
                resume_from = existing

        headers = {}
        if resume_from:
            headers["Range"] = f"bytes={resume_from}-"

        # The signed URL is pre-authenticated, so no API rate limiting
        # here -- it isn't a Fathom API endpoint. Bandwidth throttling
        # still applies: that's about the link, not the API.
        self.bandwidth.reset()
        with self.session.get(
            file_url,
            stream=True,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            headers=headers,
        ) as resp:
            resp.raise_for_status()

            if resume_from and resp.status_code == 206:
                mode = "ab"
                written = resume_from
                print(f"    resuming from {resume_from / 1e6:.0f} MB")
            else:
                if resume_from:
                    # Server ignored the Range header and is sending the
                    # whole file, so don't append onto what we have.
                    print("    (server ignored resume request -- restarting this file)")
                mode = "wb"
                written = 0

            total = expected_size or (
                int(resp.headers.get("Content-Length") or 0)
                + (resume_from if mode == "ab" else 0)
            )

            next_report = written + 100 * 1024 * 1024
            with open(tmp_path, mode) as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    f.write(chunk)
                    written += len(chunk)
                    self.bandwidth.consume(len(chunk))
                    # Some of these run well over a gigabyte; without this
                    # a long download is indistinguishable from a hang.
                    if written >= next_report:
                        if total:
                            print(f"    ...{written / 1e6:.0f} MB of {total / 1e6:.0f} MB")
                        else:
                            print(f"    ...{written / 1e6:.0f} MB")
                        next_report += 100 * 1024 * 1024

        final_size = tmp_path.stat().st_size
        if expected_size and final_size != expected_size:
            # Leave the .part alone: the next attempt resumes from it
            # rather than re-fetching megabytes already paid for.
            raise IncompleteDownload(f"got {final_size} bytes, expected {expected_size}")
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


def extract_file_size(payload):
    """Fathom reports the video's size alongside its URL. We use it to
    verify a finished transfer and to resume a partial one."""
    if not isinstance(payload, dict):
        return None
    video = payload.get("video")
    if isinstance(video, dict):
        size = video.get("file_size_bytes") or video.get("size")
        if isinstance(size, int) and size > 0:
            return size
    return None


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


def fetch_all_meetings(client, since_dt, include_text=False):
    since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    meetings = []
    cursor = None
    while True:
        page = client.list_meetings(since_iso, cursor=cursor, include_text=include_text)
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


def text_paths(output_dir, meeting):
    """Transcript and summary paths sitting beside the video, sharing its
    name, so the three files for one meeting sort together."""
    stem = build_filename(meeting)
    if stem.endswith(".mp4"):
        stem = stem[:-4]
    return output_dir / f"{stem}.txt", output_dir / f"{stem}.summary.md"


def format_transcript(meeting):
    """Render Fathom's transcript array as readable, greppable text."""
    entries = meeting.get("transcript")
    if not entries:
        return None

    title = meeting.get("title") or "(untitled)"
    started = meeting.get("recording_start_time") or meeting.get("created_at") or ""
    lines = [title]
    if started:
        lines.append(started[:10])
    lines.append("=" * max(len(title), 12))
    lines.append("")

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        speaker = (entry.get("speaker") or {}).get("display_name") or "Unknown speaker"
        text = (entry.get("text") or "").strip()
        if not text:
            continue
        timestamp = entry.get("timestamp")
        lines.append(f"[{timestamp}] {speaker}: {text}" if timestamp else f"{speaker}: {text}")

    return "\n".join(lines) + "\n"


def format_summary(meeting):
    summary = meeting.get("default_summary")
    if not isinstance(summary, dict):
        return None
    body = summary.get("markdown_formatted")
    if not body or not body.strip():
        return None

    title = meeting.get("title") or "(untitled)"
    started = meeting.get("recording_start_time") or meeting.get("created_at") or ""
    header = [f"# {title}"]
    if started:
        header.append(f"*{started[:10]}*")
    template = summary.get("template_name")
    if template:
        header.append(f"*Summary template: {template}*")
    return "\n\n".join(header) + "\n\n" + body.rstrip() + "\n"


def save_meeting_text(meetings, output_dir, overwrite=False):
    """Write transcripts and summaries next to their videos.

    Deliberately keeps no state of its own: a file either exists or it
    doesn't, and that's cheap to check. So this is safe to re-run, and
    safe to run while a video download is in progress -- it never touches
    the download state file.
    """
    wrote_transcripts = wrote_summaries = 0
    no_transcript = no_summary = 0

    for meeting in meetings:
        transcript_path, summary_path = text_paths(output_dir, meeting)

        body = format_transcript(meeting)
        if body is None:
            no_transcript += 1
        elif overwrite or not transcript_path.exists():
            transcript_path.write_text(body, encoding="utf-8")
            wrote_transcripts += 1

        body = format_summary(meeting)
        if body is None:
            no_summary += 1
        elif overwrite or not summary_path.exists():
            summary_path.write_text(body, encoding="utf-8")
            wrote_summaries += 1

    print(f"  Transcripts written: {wrote_transcripts}")
    if no_transcript:
        print(f"  No transcript available: {no_transcript}")
    print(f"  Summaries written:   {wrote_summaries}")
    if no_summary:
        print(f"  No summary available: {no_summary}")


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
    if entry.get("download_id"):
        return  # already requested from a prior sweep/run
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
    size = extract_file_size(result)
    if size:
        entry["file_size"] = size
    save_state(state_path, state)


def check_and_maybe_download(client, meeting, output_dir, state, state_path,
                             delay_between_files=0):
    """Check this recording's render status once, and download it if it's
    ready -- using a link fetched in the same breath.

    Fathom's signed URLs are time-limited, so one collected earlier in a
    long run is quite likely dead by the time its turn comes around.
    Fetching the status immediately before the transfer costs one API
    call and removes that whole class of failure.
    """
    recording_id, entry = _get_entry(state, meeting)
    filename = entry["filename"]
    dest_path = output_dir / filename

    if entry.get("status") == "completed" and dest_path.exists():
        return "skipped"
    if entry.get("permanent_failure"):
        return "pending"

    download_id = entry.get("download_id")
    if not download_id:
        return "pending"  # not yet requested (picked up on the next sweep)

    try:
        status = client.get_download_status(recording_id, download_id)
    except requests.exceptions.RequestException as e:
        print(f"  Status check failed for {recording_id}: {e}")
        return "pending"

    status_value = status.get("status")
    if status_value in ("expired", "failed", "error", "canceled", "cancelled"):
        # The download request itself died. Clear it so the next sweep
        # asks Fathom for a fresh one -- otherwise we'd poll a dead
        # request forever and never make progress.
        print(f"  Download {status_value} for {recording_id} -- requesting a fresh one")
        entry.pop("download_id", None)
        entry["status"] = "pending"
        save_state(state_path, state)
        return "pending"

    file_url = extract_file_url(status)
    if status_value != "completed" or not file_url:
        return "pending"  # still rendering; try again next sweep

    expected_size = extract_file_size(status) or entry.get("file_size")
    if expected_size:
        entry["file_size"] = expected_size

    print(f"  Downloading: {filename}")
    try:
        client.download_file(file_url, dest_path, expected_size=expected_size)
    except (requests.exceptions.RequestException, IncompleteDownload, OSError) as e:
        print(f"  Download failed for {recording_id}: {e}")
        # Keep download_id: a status check returns a freshly-signed URL
        # every time, so the link refreshes itself on the next attempt.
        # Any .part file stays on disk to resume from.
        entry["status"] = "pending"
        save_state(state_path, state)
        return "failed"

    entry["status"] = "completed"
    entry.pop("error", None)
    save_state(state_path, state)

    if delay_between_files:
        time.sleep(delay_between_files)
    return "downloaded"


def tidy_empty_part_files(output_dir):
    """Remove zero-byte .part stubs left behind by connections that died
    before sending anything. Partial files with real bytes in them are
    kept -- those are resumable, and on a slow link expensive to refetch.
    """
    removed = 0
    for part in output_dir.glob("*.part"):
        try:
            if part.stat().st_size == 0:
                part.unlink()
                removed += 1
        except OSError:
            pass
    if removed:
        print(f"Cleaned up {removed} empty .part file(s) from a previous run.\n")


def print_summary(state, meetings, output_dir, downloaded_this_run, reason):
    def entry_for(m):
        return state.get(str(m["recording_id"]), {})

    def on_disk(m):
        # Count files, not state entries. If the two disagree the file is
        # what matters, and the disagreement is worth surfacing rather
        # than papering over.
        filename = entry_for(m).get("filename")
        return bool(filename) and (output_dir / filename).exists()

    done = sum(1 for m in meetings if on_disk(m))
    claimed = sum(1 for m in meetings if entry_for(m).get("status") == "completed")
    blocked = sum(1 for m in meetings if entry_for(m).get("permanent_failure"))
    outstanding = len(meetings) - done - blocked

    print("\n" + "=" * 62)
    print(f"Stopped: {reason}")
    print("=" * 62)
    print(f"  Downloaded this run:   {downloaded_this_run}")
    print(f"  Complete on disk:      {done} of {len(meetings)}")
    if claimed > done:
        print(f"  !! {claimed - done} file(s) marked downloaded are missing from disk "
              f"-- queued for re-download.")
    print(f"  Still rendering:       {outstanding}")
    if blocked:
        print(f"  Unavailable at Fathom: {blocked} (no video; will not be retried)")
    print(f"\n  Videos saved to: {output_dir}")
    if outstanding:
        print(
            f"\n  {outstanding} recording(s) are still rendering on Fathom's side.\n"
            "  That can take many hours. Run this again later and it will pick up\n"
            "  where it left off -- nothing already downloaded is fetched twice."
        )
    else:
        print("\n  Nothing outstanding. All done.")


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
    parser.add_argument("--max-rate-kbps", type=float, default=0,
                         help="Throttle downloads to this many KB/s (0 = unlimited). "
                              "Use on a shared or metered link so the download doesn't "
                              "flatten the connection for everyone else.")
    parser.add_argument("--delay-between-files", type=float, default=0,
                         help="Seconds to pause after each video (default: 0)")
    parser.add_argument("--max-runtime-hours", type=float, default=0,
                         help="Stop cleanly after roughly this many hours (0 = no limit)")
    parser.add_argument("--max-idle-sweeps", type=int, default=3,
                         help="Give up after this many consecutive sweeps that download "
                              "nothing (default: 3). Fathom's rendering doesn't resolve "
                              "in minutes, so sweeping on is wasted effort.")
    parser.add_argument("--with-transcripts", action="store_true",
                         help="Also save each meeting's transcript (.txt) and summary "
                              "(.summary.md) beside its video")
    parser.add_argument("--transcripts-only", action="store_true",
                         help="Save transcripts and summaries and then stop, without "
                              "downloading any video. Fast, tiny, and safe to run while "
                              "a video download is going.")
    parser.add_argument("--overwrite-text", action="store_true",
                         help="Re-write transcripts/summaries that already exist "
                              "(default: leave them alone)")
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

    bandwidth = BandwidthLimiter(int(args.max_rate_kbps * 1024))
    client = FathomClient(args.api_key, bandwidth_limiter=bandwidth)

    if args.max_rate_kbps:
        print(f"Throttling downloads to {args.max_rate_kbps:.0f} KB/s.")
    tidy_empty_part_files(output_dir)

    want_text = args.with_transcripts or args.transcripts_only

    print(f"Fetching meetings recorded since {since_dt.date().isoformat()}...")
    if want_text:
        print("  (including transcripts and summaries -- pages are larger, so this is slower)")
    meetings = fetch_all_meetings(client, since_dt, include_text=want_text)
    print(f"Found {len(meetings)} meetings.\n")

    if args.list_only:
        already = sum(1 for m in meetings if state.get(str(m["recording_id"]), {}).get("status") == "completed")
        print(f"{already} already downloaded, {len(meetings) - already} remaining.")
        return

    if want_text:
        print("Saving transcripts and summaries...")
        save_meeting_text(meetings, output_dir, overwrite=args.overwrite_text)
        print()
        if args.transcripts_only:
            print(f"Done. Text files saved to: {output_dir}")
            return

    def outstanding(m):
        entry = state.get(str(m["recording_id"]), {})
        if entry.get("permanent_failure"):
            return False
        if entry.get("status") != "completed":
            return True
        # Marked done -- but trust the disk over the state file. A file
        # can go missing between runs (a cleanup, a half-finished copy,
        # a drive that wasn't mounted when the state was written), and
        # if we take "completed" at face value it is never re-fetched
        # and never even reported as outstanding. The video quietly
        # isn't there and nothing says so.
        filename = entry.get("filename")
        return not (filename and (output_dir / filename).exists())

    started = time.monotonic()

    def out_of_time():
        if not args.max_runtime_hours:
            return False
        return (time.monotonic() - started) >= args.max_runtime_hours * 3600

    # Phase 1: ask Fathom to start rendering every remaining recording up
    # front. This is what lets Fathom work on many videos in parallel on
    # their end, instead of us serializing everything by waiting on one
    # at a time.
    to_request = [
        m for m in meetings
        if outstanding(m) and not state.get(str(m["recording_id"]), {}).get("download_id")
    ]
    if to_request:
        print(f"Requesting video generation for {len(to_request)} meeting(s)...")
        for i, meeting in enumerate(to_request, 1):
            ensure_download_requested(client, meeting, state, state_path)
            if i % 20 == 0 or i == len(to_request):
                print(f"  Requested {i}/{len(to_request)}...")

    # Phase 2: sweep through everyone repeatedly, checking status once per
    # sweep and downloading whatever's ready.
    downloaded_total = 0
    sweep = 0
    idle_sweeps = 0
    reason = "nothing left to do"

    while True:
        pending_meetings = [m for m in meetings if outstanding(m)]
        if not pending_meetings:
            break
        if out_of_time():
            reason = f"hit the {args.max_runtime_hours:g}h runtime limit"
            break

        sweep += 1
        print(f"\n--- Sweep {sweep}: {len(pending_meetings)} meeting(s) remaining ---\n")
        sweep_downloaded = sweep_failed = sweep_pending = 0

        for meeting in pending_meetings:
            if out_of_time():
                break
            result = check_and_maybe_download(
                client, meeting, output_dir, state, state_path,
                delay_between_files=args.delay_between_files,
            )
            if result == "downloaded":
                sweep_downloaded += 1
                downloaded_total += 1
                print(f"  [{downloaded_total} downloaded so far] {meeting.get('title', '(untitled)')}")
            elif result == "failed":
                sweep_failed += 1
            elif result == "pending":
                sweep_pending += 1

        print(
            f"\nSweep {sweep} done: {sweep_downloaded} downloaded, "
            f"{sweep_pending} still rendering, {sweep_failed} failed."
        )

        idle_sweeps = idle_sweeps + 1 if sweep_downloaded == 0 else 0

        # Stop once it's clear nothing is becoming available. Fathom's
        # rendering takes hours, not minutes, so continuing to sweep a
        # list of still-rendering recordings just burns bandwidth and API
        # quota. Better to exit with a clear report and be re-run later.
        # (See the warning at the top of this file: a clean exit must not
        # trigger an automatic restart, or this becomes an infinite loop.)
        if idle_sweeps >= args.max_idle_sweeps:
            reason = f"{idle_sweeps} sweeps in a row downloaded nothing"
            break

        cooldown = min(
            SWEEP_COOLDOWN_SECONDS * (2 ** idle_sweeps),
            MAX_SWEEP_COOLDOWN_SECONDS,
        )
        print(f"Cooling down {cooldown}s before the next sweep...")
        time.sleep(cooldown)

    print_summary(state, meetings, output_dir, downloaded_total, reason)


if __name__ == "__main__":
    main()
