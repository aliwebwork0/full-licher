import os
import re
import shutil
import threading
import subprocess
import shlex
import time
from queue import Queue
from datetime import datetime
from urllib.parse import urlparse

# --- Global Config ---
job_queue = Queue()
jobs = {}
jobs_lock = threading.Lock()
processes = {}
processes_lock = threading.Lock()

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

RCLONE_CONFIG_PATH = "/root/.config/rclone/rclone.conf"
MAX_RETRIES    = 5
CONNECT_TIMEOUT = 30
STALL_TIMEOUT  = 60

def _parse_size_str(s):
    if not s: return 0
    s = s.strip()
    m = re.search(r'([\d.]+)\s*([KkMmGg]i?B?)', s)
    if not m: return 0
    try:
        v = float(m.group(1))
        u = m.group(2)[0].upper()
        mul = {"K": 1024, "M": 1024**2, "G": 1024**3}.get(u, 1)
        return int(v * mul)
    except: return 0

def now(): return datetime.utcnow().strftime("%H:%M:%S")

def set_job(job_id, **kwargs):
    with jobs_lock:
        if job_id in jobs: jobs[job_id].update(kwargs)

def append_log(job_id, text):
    with jobs_lock:
        if job_id in jobs: jobs[job_id]["log"] += text

def parse_progress(line):
    match = re.search(r'(\d+(?:\.\d+)?)\s*%', line)
    return float(match.group(1)) if match else None

def parse_ytdlp_progress(line):
    match = re.search(r'\[download\]\s+([\d.]+)%\s+of\s+([\d.]+\s*\S+)\s+at\s+([\d.]+\s*\S+/s)(?:\s+ETA\s+(\S+))?', line)
    if match: return float(match.group(1)), match.group(2), match.group(3), match.group(4) or ""
    return None, None, None, None

def detect_source_type(url):
    u = url.lower()
    if any(x in u for x in ["youtube.com", "youtu.be"]): return "youtube"
    if "instagram.com" in u: return "instagram"
    return "direct"

# --- اصلی‌ترین بخش: دانلود مستقیم برای CDNهای حساس ---

def build_direct_cmd(url, dest_path):
    """
    نسخه بهینه شده برای عبور از سد CDNهای حساس مثل Eporner
    """
    safe_url = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)
    rclone_cfg = shlex.quote(RCLONE_CONFIG_PATH)
    
    # استخراج دامین برای هدر Referer - حیاتی برای Eporner
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    # لینک‌های Eporner معمولاً به رفرنس خود سایت نیاز دارند
    referer = "https://www.eporner.com/" if "eporner" in url else origin

    # استفاده از curl_chrome116 برای شبیه سازی اثرانگشت TLS
    curl_bin = "curl_chrome116" if shutil.which("curl_chrome116") else "curl"
    
    # هدرهای دقیقی که مرورگر هنگام کلیک روی لینک مستقیم میفرستد
    headers = [
        f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        f"Referer: {referer}",
        f"Origin: {origin}",
        "Accept: */*",
        "Accept-Language: en-US,en;q=0.9",
        "Connection: keep-alive",
        "Sec-Fetch-Dest: video",
        "Sec-Fetch-Mode: cors",
        "Sec-Fetch-Site: same-site",
    ]
    
    header_str = " ".join([f"-H {shlex.quote(h)}" for h in headers])

    # اجرای curl و پایپ به rclone با بافر بالا و قابلیت Retry در rclone
    return (
        f"{curl_bin} -g -L -k --compressed {header_str} "
        f"--connect-timeout {CONNECT_TIMEOUT} "
        f"--speed-limit 1000 --speed-time {STALL_TIMEOUT} "
        f"{safe_url} | RCLONE_CONFIG={rclone_cfg} rclone rcat {safe_dest} "
        f"--buffer-size 64M --low-level-retries 10 --retries 3"
    )

def build_ytdlp_cmd(url, dest_path, quality="best"):
    safe_url = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)
    rclone_cfg = shlex.quote(RCLONE_CONFIG_PATH)
    fmt = "bestvideo+bestaudio/best"
    if quality == "1080p": fmt = "bestvideo[height<=1080]+bestaudio/best[height<=1080]"
    tmp = f"/tmp/ytdl_{os.getpid()}.%(ext)s"
    return (
        f"set -e; "
        f"OUTFILE=$(yt-dlp -f {shlex.quote(fmt)} --no-playlist --newline --no-check-certificate -o {shlex.quote(tmp)} --print after_move:filepath {safe_url}); "
        f"RCLONE_CONFIG={rclone_cfg} rclone rcat {safe_dest} < \"$OUTFILE\"; "
        f"rm -f \"$OUTFILE\""
    )

def run_job(job):
    job_id, url, filename = job["id"], job["url"], job["filename"]
    dest, quality = job.get("dest", "mega:/Video"), job.get("quality", "best")
    source_type = detect_source_type(url)
    dest_path = f"{dest}/{filename}"

    set_job(job_id, status="running", log="", progress=0, started_at=now(), source_type=source_type)
    
    cmd = build_ytdlp_cmd(url, dest_path, quality) if source_type != "direct" else build_direct_cmd(url, dest_path)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
            with processes_lock: processes[job_id] = p

            for line in p.stdout:
                if jobs.get(job_id, {}).get("status") == "cancelled":
                    os.killpg(os.getpgid(p.pid), 9); return

                if source_type != "direct":
                    pct, sz, spd, eta = parse_ytdlp_progress(line)
                    if pct is not None: set_job(job_id, progress=pct, speed=spd, eta=eta, filesize_str=sz)
                else:
                    match = re.search(r'(\d+)%', line)
                    if match: set_job(job_id, progress=float(match.group(1)))

                if not any(x in line for x in ['#', '=']):
                    append_log(job_id, f"[{now()}] {line.strip()}\n")

            p.wait()
            if p.returncode == 0:
                set_job(job_id, status="done", progress=100, finished_at=now())
                return
            else:
                append_log(job_id, f"[{now()}] Attempt {attempt} failed.\n")
                time.sleep(5)
        except Exception as e:
            append_log(job_id, f"Error: {e}\n")
            time.sleep(5)

    set_job(job_id, status="failed")

def worker_loop():
    while True:
        job = job_queue.get()
        try: run_job(job)
        finally: job_queue.task_done()

threading.Thread(target=worker_loop, daemon=True).start()
