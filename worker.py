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

# --- تنظیمات و متغیرهای سراسری ---
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
RETRY_DELAY    = 8
STALL_TIMEOUT  = 120
CONNECT_TIMEOUT = 30

# --- توابع کمکی (Utilities) ---

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
    return float(match.group(1)) if match else None

def parse_ytdlp_progress(line):
    match = re.search(r'\[download\]\s+([\d.]+)%\s+of\s+([\d.]+\s*\S+)\s+at\s+([\d.]+\s*\S+/s)(?:\s+ETA\s+(\S+))?', line)
    if match:
        return float(match.group(1)), match.group(2), match.group(3), match.group(4) or ""
    return None, None, None, None

def detect_source_type(url):
    u = url.lower()
    if any(x in u for x in ["youtube.com", "youtu.be"]): return "youtube"
    if "instagram.com" in u: return "instagram"
    return "direct"

# --- بخش اصلی: دانلود مستقیم (Direct Download) ---

def build_direct_cmd(url, dest_path):
    """
    این بخش برای دانلود لینک‌های مستقیم حساس طراحی شده است.
    با استفاده از شبیه‌سازی کامل هدرهای کروم و بافر سنگین rclone.
    """
    safe_url = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)
    rclone_cfg = shlex.quote(RCLONE_CONFIG_PATH)
    
    parsed = urlparse(url)
    referer = f"{parsed.scheme}://{parsed.netloc}/"
    
    # استفاده از curl-impersonate برای شبیه‌سازی اثرانگشت TLS مرورگر کروم
    curl_bin = "curl_chrome116" if shutil.which("curl_chrome116") else "curl"
    
    # هدرهای دقیق مرورگر کروم برای جلوگیری از شناسایی ربات
    headers = [
        f"Host: {parsed.netloc}",
        "Connection: keep-alive",
        "Upgrade-Insecure-Requests: 1",
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Sec-Fetch-Site: none",
        "Sec-Fetch-Mode: navigate",
        "Sec-Fetch-User: ?1",
        "Sec-Fetch-Dest: document",
        "Accept-Encoding: gzip, deflate, br",
        "Accept-Language: en-US,en;q=0.9",
        f"Referer: {referer}"
    ]
    
    header_str = " ".join([f"-H {shlex.quote(h)}" for h in headers])

    # نکته کلیدی: اضافه کردن بافر 128 مگابایتی به rclone
    # این کار باعث می‌شود اگر آپلود به مگا کند شد، دانلودِ curl متوقف نشود و لینک نسوزد.
    return (
        f"{curl_bin} -g -L -k {header_str} --compressed "
        f"--connect-timeout {CONNECT_TIMEOUT} "
        f"--speed-limit 100 --speed-time 30 "
        f"{safe_url} | RCLONE_CONFIG={rclone_cfg} rclone rcat {safe_dest} "
        f"--buffer-size 128M --mega-chunk-size 64M --transfers 1"
    )

# --- بخش دانلود از یوتیوب و اینستاگرام ---

def build_ytdlp_cmd(url, dest_path, quality="best"):
    safe_url = shlex.quote(url)
    safe_dest = shlex.quote(dest_path)
    rclone_cfg = shlex.quote(RCLONE_CONFIG_PATH)
    
    fmt = "bestvideo+bestaudio/best"
    if quality == "1080p": fmt = "bestvideo[height<=1080]+bestaudio/best[height<=1080]"
    elif quality == "720p": fmt = "bestvideo[height<=720]+bestaudio/best[height<=720]"
    
    tmp = f"/tmp/ytdl_{os.getpid()}.%(ext)s"
    return (
        f"set -e; "
        f"OUTFILE=$(yt-dlp -f {shlex.quote(fmt)} --no-playlist --newline --no-check-certificate --merge-output-format mp4 -o {shlex.quote(tmp)} --print after_move:filepath {safe_url}); "
        f"RCLONE_CONFIG={rclone_cfg} rclone rcat {safe_dest} < \"$OUTFILE\"; "
        f"rm -f \"$OUTFILE\""
    )

# --- مدیریت اجرای تسک‌ها (Job Execution) ---

def kill_job_process(job_id):
    with processes_lock:
        p = processes.pop(job_id, None)
    if p:
        try:
            import signal
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except:
            try: p.kill()
            except: pass

def run_job(job):
    job_id, url, filename = job["id"], job["url"], job["filename"]
    dest = job.get("dest", "mega:/Video")
    quality = job.get("quality", "best")
    source_type = detect_source_type(url)
    dest_path = f"{dest}/{filename}"

    set_job(job_id, status="running", log="", progress=0, retries=0, started_at=now(), source_type=source_type)
    append_log(job_id, f"[{now()}] Initializing {source_type} transfer...\n")

    if source_type in ("youtube", "instagram"):
        cmd = build_ytdlp_cmd(url, dest_path, quality)
    else:
        cmd = build_direct_cmd(url, dest_path)

    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG_PATH

    for attempt in range(1, MAX_RETRIES + 1):
        if jobs.get(job_id, {}).get("status") == "cancelled": return

        try:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, start_new_session=True)
            with processes_lock: processes[job_id] = p

            for line in p.stdout:
                if jobs.get(job_id, {}).get("status") == "cancelled":
                    kill_job_process(job_id)
                    return

                # پارس کردن پروگرس
                if source_type in ("youtube", "instagram"):
                    pct, size_str, speed_str, eta_str = parse_ytdlp_progress(line)
                    if pct is not None:
                        set_job(job_id, progress=pct, speed=speed_str, eta=eta_str, filesize_str=size_str)
                else:
                    pct = parse_progress(line)
                    if pct is not None: set_job(job_id, progress=pct)

                # لاگ کردن خروجی‌ها
                if not any(x in line for x in ['#', '=', '    ']):
                    clean = line.strip()
                    if clean: append_log(job_id, f"[{now()}] {clean}\n")

            p.wait()
            with processes_lock: processes.pop(job_id, None)

            if p.returncode == 0:
                set_job(job_id, status="done", progress=100, finished_at=now())
                with stats_lock: session_stats["jobs_done"] += 1
                return
            else:
                append_log(job_id, f"[{now()}] Error: Process exited with {p.returncode}. Attempt {attempt}/{MAX_RETRIES}\n")
                set_job(job_id, retries=attempt)
                time.sleep(RETRY_DELAY)

        except Exception as e:
            append_log(job_id, f"[{now()}] Exception: {str(e)}\n")
            time.sleep(RETRY_DELAY)

    set_job(job_id, status="failed", finished_at=now())
    with stats_lock: session_stats["jobs_failed"] += 1

def worker_loop():
    while True:
        job = job_queue.get()
        if jobs.get(job["id"], {}).get("status") == "cancelled":
            job_queue.task_done()
            continue
        try:
            run_job(job)
        except Exception as e:
            set_job(job["id"], status="failed", log=f"Fatal error: {e}")
        finally:
            job_queue.task_done()

# شروع ترد ورکر در پس‌زمینه
threading.Thread(target=worker_loop, daemon=True).start()
