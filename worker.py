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

# Session-wide stats with total download/upload tracking
session_stats = {
    "total_downloaded_bytes": 0,
    "total_uploaded_bytes": 0,
    "jobs_done": 0,
    "jobs_failed": 0,
    "current_speed_dl": 0,
    "current_speed_ul": 0,
    "history_dl": [],
    "history_ul": [],
}
stats_lock = threading.Lock()

RCLONE_CONFIG_PATH = "/root/.config/rclone/rclone.conf"

MAX_RETRIES     = 5
RETRY_DELAY     = 8
STALL_TIMEOUT   = 120
CONNECT_TIMEOUT = 30


def fmt_bytes(b):
    b = float(b)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
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


# Progress parsers
RCLONE_XFER_RE = re.compile(
    r'Transferred:\s+'
    r'([\d.]+\s*\S+)\s*/\s*([\d.]+\s*\S+),\s*'
    r'(\d+)%'
    r'(?:,\s*([\d.]+\s*\S+/s))?'
    r'(?:,\s*ETA\s*(\S+))?'
)


def parse_rclone_progress(line):
    m = RCLONE_XFER_RE.search(line)
    if m:
        recv_str = m.group(1).strip()
        total_str = m.group(2).strip()
        pct = int(m.group(3))
        speed_str = (m.group(4) or "").strip()
        eta_str = (m.group(5) or "").strip()
        return pct, recv_str, total_str, speed_str, eta_str
    return None


def parse_rclone_speed_bytes(speed_str):
    m = re.search(r'([\d.]+)\s*([KkMmGg]i?)[Bb]/s', speed_str)
    if m:
        val = float(m.group(1))
        prefix = m.group(2)[0].upper()
        mul = {"K": 1024, "M": 1024**2, "G": 1024**3}.get(prefix, 1)
        return val * mul
    return 0.0


def parse_ytdlp_progress(line):
    match = re.search(
        r'\[download\]\s+([\d.]+)%\s+of\s+([\d.]+\s*\S+)\s+at\s+([\d.]+\s*\S+/s)(?:\s+ETA\s+(\S+))?',
        line
    )
    if match:
        pct = float(match.group(1))
        size_str = match.group(2)
        speed_str = match.group(3)
        eta_str = match.group(4) or ""
        return pct, size_str, speed_str, eta_str
    return None, None, None, None


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


def add_bytes_downloaded(bytes_added):
    with stats_lock:
        session_stats["total_downloaded_bytes"] += bytes_added


def add_bytes_uploaded(bytes_added):
    with stats_lock:
        session_stats["total_uploaded_bytes"] += bytes_added


# ============================================================
# FIXED: Direct download command with unbuffered output
# ============================================================
def build_direct_cmd(url, dest_path):
    safe_url = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)
    referer = get_referer(url)

    # Use stdbuf to force line-buffered output for real-time progress
    return (
        f"stdbuf -oL -eL rclone copyurl {safe_url} {safe_dest} "
        f"--progress "
        f"--stats 1s "
        f"--stats-one-line "
        f"--header 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' "
        f"--header 'Referer: {referer}' "
        f"--retries 3 "
        f"--low-level-retries 5 "
        f"--timeout {CONNECT_TIMEOUT}s "
        f"2>&1"
    )


# ============================================================
# FIXED: YouTube/Instagram with upload progress
# ============================================================
def build_ytdlp_cmd(url, dest_path, quality="best"):
    safe_url = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)

    fmt = "bestvideo+bestaudio/best"
    if quality == "1080p":
        fmt = "bestvideo[height<=1080]+bestaudio/best[height<=1080]"
    elif quality == "720p":
        fmt = "bestvideo[height<=720]+bestaudio/best[height<=720]"
    elif quality == "480p":
        fmt = "bestvideo[height<=480]+bestaudio/best[height<=480]"
    elif quality == "audio":
        fmt = "bestaudio/best"

    tmp = f"/tmp/ytdl_{os.getpid()}.%(ext)s"
    safe_tmp = shlex.quote(tmp)

    # yt-dlp downloads to temp file, then rclone uploads with --progress
    return (
        f"export PYTHONUNBUFFERED=1; "
        f"OUTFILE=$(yt-dlp -f {shlex.quote(fmt)} "
        f"--no-playlist --newline "
        f"--merge-output-format mp4 "
        f"--progress "
        f"-o {safe_tmp} "
        f"--print after_move:filepath "
        f"{safe_url} 2>&1); "
        f"if [ -n \"$OUTFILE\" ] && [ -f \"$OUTFILE\" ]; then "
        f"  echo '[UPLOAD] Starting upload to MEGA...'; "
        f"  stdbuf -oL -eL rclone rcat --progress {safe_dest} < \"$OUTFILE\" 2>&1; "
        f"  rm -f \"$OUTFILE\"; "
        f"  echo '[UPLOAD] Upload complete!'; "
        f"fi"
    )


# ============================================================
# Main job runner with proper progress tracking
# ============================================================
def run_job(job):
    job_id = job["id"]
    url = job["url"]
    filename = job["filename"]
    dest = job.get("dest", "mega:/Video")
    quality = job.get("quality", "best")

    source_type = detect_source_type(url)
    dest_path = f"{dest}/{filename}"

    set_job(job_id, status="running", log="", progress=0, retries=0,
            started_at=now(), source_type=source_type, speed="", eta="",
            downloaded_str="0 B", filesize_str="unknown")
    append_log(job_id, f"[{now()}] Starting: {filename}\n")
    append_log(job_id, f"[{now()}] Source: {source_type.upper()} → {dest_path}\n")

    if source_type in ("youtube", "instagram"):
        cmd = build_ytdlp_cmd(url, dest_path, quality)
    else:
        cmd = build_direct_cmd(url, dest_path)

    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG_PATH
    env["PYTHONUNBUFFERED"] = "1"

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
                bufsize=1,
            )

            with processes_lock:
                processes[job_id] = p

            for line in p.stdout:
                if is_cancelled(job_id):
                    kill_job_process(job_id)
                    append_log(job_id, f"[{now()}] Cancelled mid-transfer.\n")
                    return

                line = line.rstrip('\n')
                
                # Parse yt-dlp download progress
                if source_type in ("youtube", "instagram"):
                    pct, size_str, speed_str, eta_str = parse_ytdlp_progress(line)
                    if pct is not None:
                        set_job(job_id, progress=pct, speed=speed_str or "", eta=eta_str or "")
                        if size_str:
                            set_job(job_id, filesize_str=size_str)
                        spd = parse_rclone_speed_bytes(speed_str) if speed_str else 0.0
                        update_speed_history(spd, 0)
                        add_bytes_downloaded(int(spd))
                        continue
                    
                    # Parse rclone upload progress (for the upload phase)
                    if '[UPLOAD]' in line or 'rcat' in line.lower():
                        upload_result = parse_rclone_progress(line)
                        if upload_result is not None:
                            pct, recv_str, total_str, speed_str, eta_str = upload_result
                            # For upload phase, progress is already 100% from download
                            # Just show upload speed
                            set_job(job_id, upload_speed=speed_str or "", upload_progress=pct)
                            spd = parse_rclone_speed_bytes(speed_str) if speed_str else 0.0
                            update_speed_history(0, spd)
                            add_bytes_uploaded(int(spd))
                            continue

                # Parse rclone copyurl progress (direct download + upload together)
                result = parse_rclone_progress(line)
                if result is not None:
                    pct, recv_str, total_str, speed_str, eta_str = result
                    set_job(job_id, progress=pct,
                            speed=speed_str,
                            eta=eta_str,
                            filesize_str=total_str,
                            downloaded_str=recv_str)
                    spd = parse_rclone_speed_bytes(speed_str) if speed_str else 0.0
                    # For direct, both download and upload happen simultaneously
                    update_speed_history(spd, spd)
                    add_bytes_downloaded(int(spd))
                    add_bytes_uploaded(int(spd))
                    continue

                # Log everything else
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
                set_job(job_id, status="done", progress=100,
                        finished_at=now(), speed="", eta="")
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
