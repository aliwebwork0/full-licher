import os
import re
import threading
import subprocess
import shlex
import time
from queue import Queue
from datetime import datetime

job_queue = Queue()
jobs = {}
jobs_lock = threading.Lock()
processes = {}
processes_lock = threading.Lock()

# Session-wide stats
session_stats = {
    "total_transferred_bytes": 0,
    "jobs_done": 0,
    "jobs_failed": 0,
    "current_speed_dl": 0,
    "current_speed_ul": 0,
    "history_dl": [],
    "history_ul": [],
}
stats_lock = threading.Lock()


def _parse_size_str(s):
    """Parse human-readable size string like '123.4MiB' or '1.2GiB' → bytes"""
    if not s:
        return 0
    s = s.strip()
    m = re.search(r'([\d.]+)\s*([KkMmGg]i?B?)', s)
    if not m:
        return 0
    try:
        v = float(m.group(1))
        u = m.group(2)[0].upper()
        mul = {"K": 1024, "M": 1024**2, "G": 1024**3}.get(u, 1)
        return int(v * mul)
    except Exception:
        return 0

RCLONE_CONFIG_PATH = "/root/.config/rclone/rclone.conf"

MAX_RETRIES    = 5
RETRY_DELAY    = 8
STALL_TIMEOUT  = 120
CONNECT_TIMEOUT = 30


def now():
    return datetime.utcnow().strftime("%H:%M:%S")


def iter_stream_lines(stream):
    """Read a text stream char-by-char and yield 'lines' split on either
    \\n or \\r. curl --progress-bar rewrites its progress line using \\r
    without ever emitting \\n, so the default `for line in stream` never
    yields until the whole transfer is done. This makes progress live."""
    buf = []
    while True:
        ch = stream.read(1)
        if ch == "":
            break
        if ch == "\n" or ch == "\r":
            if buf:
                yield "".join(buf)
                buf = []
        else:
            buf.append(ch)
    if buf:
        yield "".join(buf)


def fmt_bytes(n):
    try:
        n = float(n)
    except Exception:
        return ""
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if n < 1024 or unit == "TiB":
            return f"{n:.2f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.2f}TiB"


def set_job(job_id, **kwargs):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(kwargs)


def append_log(job_id, text):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["log"] += text


def parse_progress(line):
    match = re.search(r'(\d+(?:\.\d+)?)\s*%', line)
    if match:
        return float(match.group(1))
    return None


def parse_speed(line):
    """Parse speed from curl progress lines like '1.23M' or '456k'"""
    match = re.search(r'(\d+(?:\.\d+)?)\s*([KkMmGg]?)(?:\s*/s|\s*bps)?', line)
    if match:
        val = float(match.group(1))
        unit = match.group(2).upper()
        multiplier = {"K": 1024, "M": 1024**2, "G": 1024**3}.get(unit, 1)
        return val * multiplier
    return None


def parse_filesize(line):
    """Try to parse total file size from curl output"""
    # curl: '  % Total    % Received  ...'
    match = re.search(r'(\d+)\s+(\d+)\s+\d+\s+\d+', line)
    if match:
        total = int(match.group(1))
        received = int(match.group(2))
        if total > 0:
            return total, received
    return None, None


def parse_ytdlp_progress(line):
    """Parse yt-dlp progress lines"""
    # [download]  45.3% of   123.45MiB at    2.34MiB/s ETA 00:32
    match = re.search(r'\[download\]\s+([\d.]+)%\s+of\s+([\d.]+\s*\S+)\s+at\s+([\d.]+\s*\S+/s)(?:\s+ETA\s+(\S+))?', line)
    if match:
        pct = float(match.group(1))
        size_str = match.group(2)
        speed_str = match.group(3)
        eta_str = match.group(4) or ""
        return pct, size_str, speed_str, eta_str
    return None, None, None, None


def is_progress_line(line):
    stripped = line.strip()
    return bool(re.match(r'^[#=\-\s\d.%|]*$', stripped))


def get_referer(url):
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return url


def detect_source_type(url):
    if any(x in url for x in ["youtube.com", "youtu.be"]):
        return "youtube"
    if "instagram.com" in url:
        return "instagram"
    return "direct"


def build_direct_cmd(url, dest_path, progress_file):
    safe_url  = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)
    referer   = get_referer(url)
    rclone_cfg = shlex.quote(RCLONE_CONFIG_PATH)
    safe_prog = shlex.quote(progress_file)

    return (
        f"curl -g -L "
        f"--connect-timeout {CONNECT_TIMEOUT} "
        f"--retry 3 --retry-delay 5 --retry-all-errors "
        f"--speed-limit 1 --speed-time {STALL_TIMEOUT} "
        f"--keepalive-time 30 "
        f"--max-time 0 "
        f"-H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' "
        f"-H 'Referer: {referer}' "
        f"-H 'Accept: */*' "
        f"-H 'Accept-Language: en-US,en;q=0.9' "
        f"-H 'Connection: keep-alive' "
        f"--fail "
        f"{safe_url} 2>{safe_prog} | RCLONE_CONFIG={rclone_cfg} rclone rcat {safe_dest}"
    )


def build_ytdlp_cmd(url, dest_path, quality="best"):
    safe_url  = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)
    rclone_cfg = shlex.quote(RCLONE_CONFIG_PATH)

    fmt = "bestvideo+bestaudio/best"
    if quality == "1080p":
        fmt = "bestvideo[height<=1080]+bestaudio/best[height<=1080]"
    elif quality == "720p":
        fmt = "bestvideo[height<=720]+bestaudio/best[height<=720]"
    elif quality == "480p":
        fmt = "bestvideo[height<=480]+bestaudio/best[height<=480]"
    elif quality == "audio":
        fmt = "bestaudio/best"

    # yt-dlp merges video+audio via ffmpeg into mkv/mp4 to a temp file,
    # then rclone uploads it. Avoids broken pipe from simultaneous pipe+merge.
    tmp = f"/tmp/ytdl_{os.getpid()}.%(ext)s"
    safe_tmp_pattern = shlex.quote(tmp)
    # We use a shell script: download to tmp, then rcat, then cleanup
    return (
        f"set -e; "
        f"OUTFILE=$(yt-dlp -f {shlex.quote(fmt)} "
        f"--no-playlist --newline "
        f"--merge-output-format mp4 "
        f"-o {safe_tmp_pattern} "
        f"--print after_move:filepath "
        f"{safe_url}); "
        f"RCLONE_CONFIG={rclone_cfg} rclone rcat {safe_dest} < \"$OUTFILE\"; "
        f"rm -f \"$OUTFILE\""
    )


def kill_job_process(job_id):
    with processes_lock:
        p = processes.pop(job_id, None)
    if p:
        try:
            import signal
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass


RCLONE_TABLE_RE = re.compile(
    r'^\s*(\d+)\s+([\d.]+[KMGTkmgt]?)\s+\d+\s+([\d.]+[KMGTkmgt]?)\s+\d+\s+\d+\s+([\d.]+[KMGTkmgt]?)'
)


def parse_curl_table_row(line):
    """Parse one row of curl's classic progress table:
    % Total  % Received % Xferd  Average Speed  Time Time Time  Current
                                  Dload  Upload   Total Spent  Left  Speed
    Returns dict or None."""
    m = RCLONE_TABLE_RE.match(line)
    if not m:
        return None
    pct_val = int(m.group(1))
    total_s = m.group(2)
    spent_s = m.group(3)
    speed_s = m.group(4)
    try:
        spd_b = _parse_size_str(speed_s + "B")
        tot_b = _parse_size_str(total_s + "B")
        spt_b = _parse_size_str(spent_s + "B")
        left_b = max(0, tot_b - spt_b)
        left_secs = int(left_b / spd_b) if spd_b else 0
        left_str = f"{left_secs//60}m{left_secs%60:02d}s" if left_secs > 0 else "—"
    except Exception:
        spd_b = 0
        left_str = "—"
    return {
        "pct": pct_val,
        "total": total_s,
        "spent": spent_s,
        "left": left_str,
        "speed": speed_s + "/s",
        "speed_bytes": spd_b,
    }


def tail_progress_file(job_id, path, stop_event):
    """Poll the curl progress file and push the latest table row into the
    job state every ~0.4s, so the UI shows the same live table the user
    sees in the Railway console."""
    last_row = None
    while not stop_event.is_set():
        try:
            with open(path, "r", errors="ignore") as f:
                data = f.read()
        except Exception:
            data = ""
        if data:
            chunks = re.split(r'[\r\n]+', data)
            for chunk in reversed(chunks):
                row = parse_curl_table_row(chunk)
                if row:
                    if row != last_row:
                        last_row = row
                        with jobs_lock:
                            if job_id in jobs:
                                rows = jobs[job_id].setdefault("rclone_rows", [])
                                rows.append(row)
                                if len(rows) > 500:
                                    del rows[:len(rows)-500]
                        set_job(job_id,
                            progress=row["pct"],
                            rclone_progress={
                                "pct":   row["pct"],
                                "total": row["total"],
                                "spent": row["spent"],
                                "left":  row["left"],
                                "speed": row["speed"],
                            },
                            speed=row["speed"]
                        )
                        update_speed_history(row["speed_bytes"], 0)
                    break
        stop_event.wait(0.4)
    with jobs_lock:
        return jobs.get(job_id, {}).get("status") == "cancelled"


def is_cancelled(job_id):
    with jobs_lock:
        return jobs.get(job_id, {}).get("status") == "cancelled"


def update_speed_history(speed_dl, speed_ul):
    with stats_lock:
        session_stats["current_speed_dl"] = speed_dl
        session_stats["current_speed_ul"] = speed_ul
        h_dl = session_stats["history_dl"]
        h_ul = session_stats["history_ul"]
        h_dl.append(speed_dl)
        h_ul.append(speed_ul)
        if len(h_dl) > 40:
            h_dl.pop(0)
        if len(h_ul) > 40:
            h_ul.pop(0)


def add_bytes_done(dl_bytes, ul_bytes):
    with stats_lock:
        session_stats["total_downloaded_bytes"] += dl_bytes
        session_stats["total_uploaded_bytes"] += ul_bytes


def run_job(job):
    job_id   = job["id"]
    url      = job["url"]
    filename = job["filename"]
    dest     = job.get("dest", "mega:/Video")
    quality  = job.get("quality", "best")

    source_type = detect_source_type(url)
    dest_path = f"{dest}/{filename}"

    set_job(job_id, status="running", log="", progress=0, retries=0,
            started_at=now(), source_type=source_type, speed="", eta="",
            rclone_progress=None, rclone_rows=[])
    append_log(job_id, f"[{now()}] Starting: {filename}\n")
    append_log(job_id, f"[{now()}] Source: {source_type.upper()} → {dest_path}\n")

    # For direct downloads, try a quick HEAD to get content-length
    if source_type == "direct":
        try:
            import urllib.request
            req = urllib.request.Request(url, method="HEAD")
            req.add_header("User-Agent", "Mozilla/5.0")
            with urllib.request.urlopen(req, timeout=8) as resp:
                cl = resp.headers.get("Content-Length")
                if cl and cl.isdigit() and int(cl) > 0:
                    set_job(job_id, filesize=int(cl), filesize_str=fmt_bytes(int(cl)))
        except Exception:
            pass

    if source_type in ("youtube", "instagram"):
        cmd = build_ytdlp_cmd(url, dest_path, quality)
        progress_file = None
    else:
        progress_file = f"/tmp/curlprog_{job_id}.log"
        cmd = build_direct_cmd(url, dest_path, progress_file)

    append_log(job_id, f"[{now()}] CMD: {cmd[:200]}{'...' if len(cmd)>200 else ''}\n")

    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG_PATH

    for attempt in range(1, MAX_RETRIES + 1):
        if is_cancelled(job_id):
            append_log(job_id, f"[{now()}] Cancelled.\n")
            return

        try:
            p = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                start_new_session=True,
            )

            with processes_lock:
                processes[job_id] = p

            tail_stop = threading.Event()
            tail_thread = None
            if progress_file:
                try:
                    open(progress_file, "a").close()
                except Exception:
                    pass
                tail_thread = threading.Thread(
                    target=tail_progress_file, args=(job_id, progress_file, tail_stop), daemon=True
                )
                tail_thread.start()

            last_dl_bytes = 0
            last_speed = 0.0

            for line in iter_stream_lines(p.stdout):
                if is_cancelled(job_id):
                    kill_job_process(job_id)
                    tail_stop.set()
                    append_log(job_id, f"[{now()}] Cancelled mid-transfer.\n")
                    return

                # yt-dlp progress parsing
                if source_type in ("youtube", "instagram"):
                    pct, size_str, speed_str, eta_str = parse_ytdlp_progress(line)
                    if pct is not None:
                        set_job(job_id, progress=pct, speed=speed_str or "", eta=eta_str or "")
                        if size_str:
                            set_job(job_id, filesize_str=size_str)
                        spd = 0
                        if speed_str:
                            m = re.search(r'([\d.]+)\s*([KkMmGg]?)iB/s', speed_str)
                            if m:
                                v = float(m.group(1))
                                u = m.group(2).upper()
                                mul = {"K":1024,"M":1024**2,"G":1024**3}.get(u,1)
                                spd = v * mul
                        update_speed_history(spd, spd * 0.95)
                        continue

                # rclone/curl progress table — MUST run before is_progress_line filter
                # curl --progress-bar table format:
                #   % Total    % Received  % Xferd  Average Speed   Time    Time     Time  Current
                #                                    Dload  Upload   Total   Spent    Left  Speed
                #  5  1.34G  5  81.23M  0  0  79.25M  0 --:--:-- --:--:-- --:--:-- 79.20M
                # Simplified rows (after header scrolls off):
                #  45  1.34G  45  625.0M  0  0  85.34M  0 ...
                # We match the core numeric block at start of line:
                rclone_match = re.match(
                    r'^\s*(\d+)\s+([\d.]+[KMGTkmgt]?)\s+\d+\s+([\d.]+[KMGTkmgt]?)\s+\d+\s+\d+\s+([\d.]+[KMGTkmgt]?)',
                    line
                )
                if rclone_match:
                    pct_val  = int(rclone_match.group(1))
                    total_s  = rclone_match.group(2)
                    spent_s  = rclone_match.group(3)
                    speed_s  = rclone_match.group(4)

                    def fmt_size(s):
                        """Add B suffix for display if missing"""
                        return s if s[-1].isalpha() else s + "B"

                    try:
                        spd_b  = _parse_size_str(speed_s + "B") or 1
                        tot_b  = _parse_size_str(total_s + "B")
                        spt_b  = _parse_size_str(spent_s + "B")
                        left_b = max(0, tot_b - spt_b)
                        left_secs = int(left_b / spd_b) if spd_b else 0
                        left_str  = f"{left_secs//60}m{left_secs%60:02d}s" if left_secs > 0 else "—"
                    except Exception:
                        left_str = "—"

                    set_job(job_id,
                        progress=pct_val,
                        rclone_progress={
                            "pct":   pct_val,
                            "total": total_s,
                            "spent": spent_s,
                            "left":  left_str,
                            "speed": speed_s + "/s",
                        },
                        speed=speed_s + "/s"
                    )
                    update_speed_history(spd_b, 0)
                    continue

                # curl progress % only
                progress = parse_progress(line)
                if progress is not None:
                    set_job(job_id, progress=progress)

                if not is_progress_line(line):
                    clean = line.strip()
                    if clean:
                        append_log(job_id, f"[{now()}] {clean}\n")

            p.wait()
            tail_stop.set()
            if progress_file:
                try:
                    os.remove(progress_file)
                except Exception:
                    pass

            with processes_lock:
                processes.pop(job_id, None)

            if is_cancelled(job_id):
                append_log(job_id, f"[{now()}] Cancelled.\n")
                return

            if p.returncode == 0:
                set_job(job_id, status="done", progress=100, finished_at=now(), speed="", eta="")
                append_log(job_id, f"[{now()}] ✓ Transfer complete.\n")
                update_speed_history(0, 0)
                # Track transferred bytes from filesize
                with jobs_lock:
                    job_snapshot = dict(jobs.get(job_id, {}))
                with stats_lock:
                    session_stats["jobs_done"] += 1
                    fs = job_snapshot.get("filesize")
                    if fs and isinstance(fs, (int, float)) and fs > 0:
                        session_stats["total_transferred_bytes"] += int(fs)
                    elif job_snapshot.get("filesize_str"):
                        session_stats["total_transferred_bytes"] += _parse_size_str(job_snapshot["filesize_str"])
                return
            else:
                append_log(job_id, f"[{now()}] ✗ Failed (exit {p.returncode}) — attempt {attempt}/{MAX_RETRIES}\n")
                set_job(job_id, retries=attempt)

                if attempt < MAX_RETRIES:
                    append_log(job_id, f"[{now()}] Retrying in {RETRY_DELAY}s...\n")
                    for _ in range(RETRY_DELAY * 2):
                        if is_cancelled(job_id):
                            append_log(job_id, f"[{now()}] Cancelled during retry wait.\n")
                            return
                        time.sleep(0.5)

        except Exception as e:
            if 'tail_stop' in dir():
                tail_stop.set()
            with processes_lock:
                processes.pop(job_id, None)
            append_log(job_id, f"[{now()}] Exception: {e} — attempt {attempt}/{MAX_RETRIES}\n")
            set_job(job_id, retries=attempt)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    set_job(job_id, status="failed", finished_at=now())
    update_speed_history(0, 0)
    with stats_lock:
        session_stats["jobs_failed"] += 1
    append_log(job_id, f"[{now()}] ✗ All {MAX_RETRIES} attempts failed.\n")


def worker_loop():
    while True:
        job = job_queue.get()
        job_id = job["id"]
        try:
            if is_cancelled(job_id):
                job_queue.task_done()
                continue
            run_job(job)
        except Exception as e:
            with jobs_lock:
                if job_id in jobs:
                    jobs[job_id]["status"] = "failed"
                    jobs[job_id]["log"] += f"[{now()}] Fatal: {e}\n"
        finally:
            job_queue.task_done()


threading.Thread(target=worker_loop, daemon=True).start()

if __name__ == "__main__":
    threading.Event().wait()
