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
    "total_downloaded_bytes": 0,
    "total_uploaded_bytes": 0,
    "jobs_done": 0,
    "jobs_failed": 0,
    "current_speed_dl": 0,
    "current_speed_ul": 0,
    "history_dl": [],   # last 30 data points (bytes/s)
    "history_ul": [],
}
stats_lock = threading.Lock()

RCLONE_CONFIG_PATH = "/root/.config/rclone/rclone.conf"

MAX_RETRIES    = 5
RETRY_DELAY    = 8
STALL_TIMEOUT  = 120
CONNECT_TIMEOUT = 30


def fmt_bytes(b):
    b = float(b)
    for unit in ['B','KB','MB','GB','TB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def fmt_bytes_speed(bps):
    return fmt_bytes(bps) + "/s"


def now():
    return datetime.utcnow().strftime("%H:%M:%S")


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


def build_direct_cmd(url, dest_path):
    safe_url  = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)
    referer   = get_referer(url)
    rclone_cfg = shlex.quote(RCLONE_CONFIG_PATH)

    return (
        f"curl -L "
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
        f"--no-buffer "
        f"--fail "
        f"{safe_url} 2>&1 | RCLONE_CONFIG={rclone_cfg} rclone rcat {safe_dest}"
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
            started_at=now(), source_type=source_type, speed="", eta="")
    append_log(job_id, f"[{now()}] Starting: {filename}\n")
    append_log(job_id, f"[{now()}] Source: {source_type.upper()} → {dest_path}\n")

    if source_type in ("youtube", "instagram"):
        cmd = build_ytdlp_cmd(url, dest_path, quality)
    else:
        cmd = build_direct_cmd(url, dest_path)

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

            last_dl_bytes = 0
            last_speed = 0.0

            for line in p.stdout:
                if is_cancelled(job_id):
                    kill_job_process(job_id)
                    append_log(job_id, f"[{now()}] Cancelled mid-transfer.\n")
                    return

                # yt-dlp progress parsing
                if source_type in ("youtube", "instagram"):
                    pct, size_str, speed_str, eta_str = parse_ytdlp_progress(line)
                    if pct is not None:
                        set_job(job_id, progress=pct, speed=speed_str or "", eta=eta_str or "")
                        if size_str:
                            set_job(job_id, filesize_str=size_str)
                        # estimate speed bytes
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

                # curl --progress-bar lines look like:
                # ######################################################################## 100.0%
                # curl -# verbose lines:
                # " 45 1234k   45  567k    0     0  234k      0  0:00:05  0:00:02  0:00:02  234k"
                progress = parse_progress(line)
                if progress is not None:
                    set_job(job_id, progress=progress)

                # Try to parse curl verbose transfer line for speed + bytes
                curl_match = re.match(
                    r'\s*\d+\s+(\d+[kKmMgG]?)\s+\d+\s+(\d+[kKmMgG]?)\s+\d+\s+\d+\s+'
                    r'(\d+[kKmMgG]?)\s+', line
                )
                if curl_match:
                    def _parse_curl_size(s):
                        s = s.strip()
                        mul = {'k': 1024, 'm': 1024**2, 'g': 1024**3}
                        if s and s[-1].lower() in mul:
                            return float(s[:-1]) * mul[s[-1].lower()]
                        return float(s) if s else 0
                    total_b  = _parse_curl_size(curl_match.group(1))
                    recv_b   = _parse_curl_size(curl_match.group(2))
                    speed_b  = _parse_curl_size(curl_match.group(3))
                    if speed_b > 0:
                        spd_str = fmt_bytes_speed(speed_b)
                        set_job(job_id, speed=spd_str,
                                downloaded=int(recv_b),
                                filesize=int(total_b) if total_b > 0 else None,
                                filesize_str=fmt_bytes(total_b) if total_b > 0 else None)
                        update_speed_history(speed_b, speed_b * 0.95)

                if not is_progress_line(line):
                    clean = line.strip()
                    if clean:
                        append_log(job_id, f"[{now()}] {clean}\n")

            p.wait()

            with processes_lock:
                processes.pop(job_id, None)

            if is_cancelled(job_id):
                append_log(job_id, f"[{now()}] Cancelled.\n")
                return

            if p.returncode == 0:
                set_job(job_id, status="done", progress=100, finished_at=now(), speed="", eta="")
                append_log(job_id, f"[{now()}] ✓ Transfer complete.\n")
                update_speed_history(0, 0)
                with stats_lock:
                    session_stats["jobs_done"] += 1
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
