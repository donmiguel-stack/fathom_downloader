# Fathom Video Downloader

Bulk-download every video recording from a [Fathom](https://fathom.video)
account using Fathom's official public API.

Fathom has no "export everything" button. If you want your recordings on
your own disk — leaving the platform, keeping an archive, handing a client
their sessions — the only way to do it is one recording at a time through
the UI, or through the API. This script does the API route, unattended, and
resumes cleanly if it's interrupted.

Two ways to run it:

- **[On your computer](#option-a-run-it-on-your-computer)** — simplest. One
  Python script. Good for a few dozen recordings, or when you just want the
  files and you're happy to leave a terminal open.
- **[On a home server via Docker](#option-b-run-it-on-a-server-umbrel-nas-raspberry-pi)** —
  for large libraries. Runs 24/7 in the background, survives reboots, doesn't
  tie up your laptop. Tested on [Umbrel](https://umbrel.com), but it's plain
  Docker Compose and will run anywhere.

Both paths use the same `fathom_downloader.py`, so a fix to one is a fix to
both.

---

## What you get

Videos land in your chosen folder, named by date and meeting title:

```
2026-08-12 - Mentor Annemarie (172549586).mp4
2026-07-14 - Impromptu Zoom Meeting (163581888).mp4
2026-04-06 - Weekly Standup (135638912).mp4
```

Sizes vary a lot — a 30-minute call might be 130 MB, a two-hour screen share
1.3 GB. **A two-year library can easily exceed 100 GB.** Check your free
space before starting.

---

## First: get an API key

1. Log in to Fathom.
2. Go to **Settings → API Access**.
3. Generate a key and copy it.

> **Note on plans:** API access isn't available on every Fathom tier. If you
> don't see the API Access section, that's why — check your plan or ask
> Fathom support.

### Where to put it

**No API key ships with this repo, and none should ever be committed to it.**
The key is yours; you supply it at runtime. There are two places to put it,
depending on how you're running things.

**Running on your computer** — set it as an environment variable in the
terminal you're about to run the script from:

```bash
export FATHOM_API_KEY="paste-your-key-here"        # macOS / Linux
```
```powershell
$env:FATHOM_API_KEY = "paste-your-key-here"        # Windows PowerShell
```

This lasts only for that terminal session, which is a feature — nothing is
written to disk. To make it permanent, add the `export` line to your
`~/.zshrc` or `~/.bashrc`.

You can also pass it directly with `--api-key`, though be aware that on a
shared machine this leaves the key visible in your shell history and in the
process list.

**Running under Docker** — copy the template and edit the copy:

```bash
cp .env.example .env
nano .env        # replace the placeholder with your real key
```

`docker-compose.yml` reads `.env` automatically. **`.env` is listed in
`.gitignore`, so git will not track it** — that's deliberate, and worth
leaving alone. `.env.example` is the committed template and contains only a
placeholder.

### Treat the key like a password

It grants read and download access to **every recording in the account** —
which, for meeting recordings, means other people's voices and words as well
as your own. So:

- Don't commit it, paste it into an issue, or put it in a screenshot.
- Don't hardcode it into `fathom_downloader.py`.
- Revoke and regenerate it in Settings → API Access if it's ever exposed.

If you think you may have committed a key already, rotate it in Fathom
immediately. Removing it in a later commit does not help — it stays readable
in the repository history.

---

## Option A: Run it on your computer

**Requirements:** Python 3.8+ and the `requests` library.

```bash
git clone https://github.com/donmiguel-stack/fathom_downloader.git
cd fathom_downloader
pip install requests
```

Set your key and run:

```bash
export FATHOM_API_KEY="paste-your-key-here"
python3 fathom_downloader.py
```

On Windows (PowerShell):

```powershell
$env:FATHOM_API_KEY = "paste-your-key-here"
python3 fathom_downloader.py
```

That downloads the last 2 years into a `fathom_videos/` folder. To check the
scale of the job before committing to it:

```bash
python3 fathom_downloader.py --list-only
```

### Options

| Flag | What it does |
|---|---|
| `--output-dir DIR` | Where to save videos (default: `./fathom_videos`) |
| `--years N` | How many years back to go (default: `2`) |
| `--since YYYY-MM-DD` | Exact start date, instead of `--years` |
| `--list-only` | Count what would be downloaded, download nothing |
| `--api-key KEY` | Pass the key directly instead of via environment |
| `--max-rate-kbps N` | Throttle to N KB/s (default: unlimited) |
| `--delay-between-files N` | Pause N seconds after each video |
| `--max-runtime-hours N` | Stop cleanly after N hours |
| `--max-idle-sweeps N` | Give up after N fruitless sweeps (default: `3`) |

**On a shared connection, throttle it.** Downloading flat-out will
saturate a household link for as long as it runs — on satellite or
anything metered, that's the difference between "a job running in the
background" and "nobody else can use the internet":

```bash
python3 fathom_downloader.py --max-rate-kbps 300
```

Examples:

```bash
# Everything since the start of 2024, onto an external drive
python3 fathom_downloader.py --since 2024-01-01 --output-dir /Volumes/Archive/fathom

# Just the last 6 months
python3 fathom_downloader.py --years 0.5
```

### Leaving it running, and running it again

Large libraries take hours. The script is safe to interrupt — `Ctrl+C`, close
the laptop, lose wifi — and re-running the same command picks up exactly
where it left off. Anything already downloaded is skipped, and a half-
transferred file resumes from where it stopped rather than starting over.

**Expect to run it more than once.** Fathom renders videos on their side,
on their schedule, and for a large library many recordings simply won't be
ready during your first run. Rather than sweeping a list of unrendered
recordings forever, the script stops after a few fruitless passes and tells
you what's outstanding:

```
Stopped: 3 sweeps in a row downloaded nothing
  Downloaded this run:   66
  Complete on disk:      66 of 237
  Still rendering:       171
```

That's not a failure — it's the expected shape of a big export. Run it again
in a few hours and those 171 will have made progress.

If you'd rather it survive your terminal closing:

```bash
nohup python3 fathom_downloader.py > fathom.log 2>&1 &
tail -f fathom.log
```

---

## Option B: Run it on a server (Umbrel, NAS, Raspberry Pi)

Better for big libraries: it runs in the background indefinitely, restarts
itself if the machine reboots, and downloads straight to whatever storage
your server has.

**Requirements:** Docker and Docker Compose, plus SSH access.

### 1. Get the files onto the server

```bash
git clone https://github.com/donmiguel-stack/fathom_downloader.git
cd fathom_downloader
```

(No git on the server? Clone locally and `scp -r` the folder over.)

### 2. Add your API key

```bash
cp .env.example .env
nano .env        # paste your key, Ctrl+O to save, Ctrl+X to exit
```

### 3. Choose where videos land

Edit the `volumes:` line in `docker-compose.yml`. The default writes to a
`videos/` folder next to the compose file. To use an external drive:

```yaml
volumes:
  - /mnt/data/fathom-videos:/data
```

Check you have room first:

```bash
df -h /mnt/data
```

### 4. Check the throttle and the restart policy

Two settings in `docker-compose.yml` are worth understanding before you
start, because both have bitten this project in production.

**`--max-rate-kbps 300`** throttles the download. Delete it on a fast,
unshared connection; keep or lower it on satellite, tethered, or metered
links, where an unthrottled download makes the internet unusable for
everyone else in the building.

**`restart: on-failure:5`** — do **not** change this to `unless-stopped` or
`always`. The script exits cleanly once several sweeps in a row find nothing
new. `unless-stopped` restarts on *any* exit, including that clean one, so
the whole run relaunches from scratch every few minutes indefinitely. In the
logs it looks like healthy activity; in reality it never finishes anything.
`on-failure` restarts only after a genuine crash, which is what you want.

### 5. Build and start

```bash
docker compose build
docker compose up -d
docker compose logs -f
```

`Ctrl+C` stops watching the logs. It does **not** stop the download — that
keeps running in the background.

When the container exits on its own, check the summary — if it reports
recordings still rendering, run `docker compose up -d` again later to
collect them.

### 5. Check on it later

```bash
ls /mnt/data/fathom-videos/*.mp4 | wc -l   # how many are done
docker compose logs --tail 50               # recent activity
docker compose ps                           # still running?
docker compose stop                         # pause (resumes where it left off)
docker compose start                        # resume
docker compose down                         # stop and remove the container
```

### A note on memory

`docker-compose.yml` caps the container at 400 MB. This is deliberate: home
servers like Umbrel are often already running memory-hungry services, and on
a 4 GB Raspberry Pi an unbounded process can trigger the OOM killer and take
down something you care about a lot more than a video download. The cap costs
nothing here — the script streams to disk in half-megabyte chunks and never
holds a video in memory. Raise or remove it on a machine with RAM to spare.

---

## How it works

Fathom's download API is asynchronous and two-step:

1. `POST /recordings/{id}/download` asks Fathom to prepare a video, and
   returns a `download_id`.
2. `GET /recordings/{id}/downloads/{download_id}` reports status, and once
   it's `completed`, includes a signed, time-limited URL to the actual file.

The script therefore runs in two phases. **Phase 1** asks Fathom to prepare
every outstanding recording, up front. **Phase 2** sweeps the full list
repeatedly, downloading whatever became ready since the last pass and backing
off between sweeps.

The up-front batching in phase 1 matters more than it looks. Requesting one
video, waiting for it, then requesting the next serializes work Fathom is
perfectly happy to do in parallel — the difference between hours and days on
a large library.

**Sweeping stops.** After `--max-idle-sweeps` consecutive passes that download
nothing, the script exits with a summary rather than sweeping on. Fathom's
rendering resolves over hours, not minutes, so a fourth identical pass over
171 unrendered recordings costs bandwidth and API quota and returns nothing.
Exiting and being re-run later is strictly better — hence the restart-policy
warning above, which is the sharp edge of this design.

**Links are fetched just in time.** A signed URL collected at the start of a
long run is very likely dead by the time its turn comes. So the status call
happens immediately before each transfer, and the URL it returns is used at
once. Conveniently, re-checking status on the same `download_id` returns a
freshly signed URL each time, so an expired link costs one API call rather
than a whole new render request.

**Partial files resume.** Downloads stream to a `.part` file that's renamed
only on success, so an interrupted transfer can never be mistaken for a
finished video. A later run sends a `Range` header and continues from where
it stopped — which matters when a 600 MB file dies at 80% on a slow link. If
the server ignores the range request, the file restarts cleanly rather than
appending onto existing bytes. Finished transfers are size-checked against
what Fathom reported; a mismatch keeps the `.part` and retries later.

Rate limiting is built in — roughly 20 requests/minute against the recordings
endpoints and 50/minute overall, comfortably under Fathom's documented caps.
The video transfers go to signed storage URLs that don't count against that
limit, so they run at whatever `--max-rate-kbps` allows.

Progress is tracked in `.fathom_download_state.json` inside your output
folder. Delete it only if you want to start completely over.

---

## Troubleshooting

**It exited saying "3 sweeps in a row downloaded nothing."**
Working as intended. Fathom hasn't finished rendering the rest yet, and
sweeping on wouldn't change that. Check the "Still rendering: N" line, come
back in a few hours, and run it again.

**It restarts from the beginning every few minutes, forever.**
Your restart policy is `unless-stopped` or `always`. Those restart the
container on *any* exit, including the clean one above, so the run relaunches
endlessly and never completes. Change it to `on-failure` in
`docker-compose.yml` and rebuild. This is the single most confusing failure
mode of the whole project, because the logs look busy and productive.

**The download makes the rest of my internet unusable.**
Throttle it: `--max-rate-kbps 300` (or lower). Unthrottled, this will take
every byte your connection can give it for hours at a stretch.

**Lots of `.part` files and read timeouts.**
Symptoms of a saturated or unreliable link rather than a bug. The `.part`
files are deliberate — they hold partial progress so a later run resumes
instead of refetching. Throttling usually reduces the timeouts too, since a
flattened connection is where most of them come from.

**Nothing downloads; the log just repeats "still rendering."**
Check what Fathom actually reports for one recording:

```bash
curl -s -H "X-Api-Key: $FATHOM_API_KEY" \
  "https://api.fathom.ai/external/v1/recordings/RECORDING_ID/downloads/DOWNLOAD_ID" \
  | python3 -m json.tool
```

If that shows `"status": "completed"`, the videos are ready and the problem
is on the client side, not Fathom's.

**`422 Unprocessable Entity` on a specific recording.**
Fathom has no downloadable video for it — typically audio-only, or a capture
that failed. The script marks it and skips it rather than retrying forever.
Expect the occasional one in a large library.

**`401 Unauthorized`.**
Bad or expired key. Regenerate it in Settings → API Access.

**Container exits with code 137.**
That's an OOM kill. Something on the machine ran out of memory — see the
memory note above, and check whether the host itself is under pressure with
`free -h`.

**`source .env` doesn't make the key visible to Python.**
`source` sets a shell variable but doesn't export it to child processes. Use:

```bash
set -a; source .env; set +a
```

**Downloads start but fail partway with an expired-link error.**
Signed URLs are time-limited. The script drops the stale link and re-requests
a fresh one on the next sweep, so this is self-healing — just let it run.

---

## Contributing

Issues and pull requests welcome. This was built against Fathom's API as it
behaved in 2026; if their response shape changes, `extract_file_url()` in
`fathom_downloader.py` is the first place to look — it deliberately accepts
several spellings of the video URL field for exactly that reason.

## License

MIT — see [LICENSE](LICENSE).

Not affiliated with or endorsed by Fathom. It uses their documented public
API. You're responsible for having the right to download the recordings you
point it at, and for handling them appropriately once you do — meeting
recordings usually contain other people's voices and words.
