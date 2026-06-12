from flask import Flask, request, render_template, jsonify
import uuid
import subprocess
import shlex
from worker import job_queue, jobs, jobs_lock, processes, processes_lock, session_stats, stats_lock

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start():
    url      = request.form.get("url", "").strip()
    filename = request.form.get("filename", "").strip()
    dest     = request.form.get("dest", "mega:/Video").strip()
    quality  = request.form.get("quality", "best").strip()

    if not url or not filename:
        return jsonify({"error": "URL and filename are required"}), 400
    if not url.startswith("http://") and not url.startswith("https://"):
        return jsonify({"error": "URL must start with http:// or https://"}), 400

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "status":      "queued",
            "log":         "",
            "url":         url,
            "filename":    filename,
            "dest":        dest,
            "quality":     quality,
            "progress":    0,
            "retries":     0,
            "started_at":  None,
            "finished_at": None,
            "filesize":    None,
            "downloaded":  0,
            "speed":       "",
            "eta":         "",
            "source_type": "direct",
        }

    job_queue.put({"id": job_id, "url": url, "filename": filename, "dest": dest, "quality": quality})
    return jsonify({"job_id": job_id})


@app.route("/status/<job_id>")
def status(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    return jsonify(job)


@app.route("/jobs")
def all_jobs():
    with jobs_lock:
        snapshot = dict(jobs)
    return jsonify(snapshot)


@app.route("/stats")
def get_stats():
    with stats_lock:
        s = dict(session_stats)
    return jsonify(s)


@app.route("/cancel/<job_id>", methods=["POST"])
def cancel(job_id):
    with jobs_lock:
        if job_id not in jobs:
            return jsonify({"error": "not found"}), 404
        jobs[job_id]["status"] = "cancelled"

    with processes_lock:
        p = processes.get(job_id)
    if p:
        try:
            p.kill()
        except Exception:
            pass

    return jsonify({"ok": True})


@app.route("/delete/<job_id>", methods=["POST"])
def delete(job_id):
    with jobs_lock:
        if job_id not in jobs:
            return jsonify({"error": "not found"}), 404
        jobs[job_id]["status"] = "cancelled"

    with processes_lock:
        p = processes.pop(job_id, None)
    if p:
        try:
            p.kill()
        except Exception:
            pass

    with jobs_lock:
        jobs.pop(job_id, None)

    return jsonify({"ok": True})


@app.route("/delete-completed", methods=["POST"])
def delete_completed():
    with jobs_lock:
        to_remove = [jid for jid, j in jobs.items() if j["status"] in ("done", "failed", "cancelled")]
        for jid in to_remove:
            jobs.pop(jid, None)
    return jsonify({"removed": len(to_remove)})


@app.route("/delete-all", methods=["POST"])
def delete_all():
    with processes_lock:
        for p in processes.values():
            try:
                p.kill()
            except Exception:
                pass
        processes.clear()

    with jobs_lock:
        count = len(jobs)
        jobs.clear()

    return jsonify({"removed": count})


@app.route("/mega/ls")
def mega_ls():
    path = request.args.get("path", "mega:/")
    import os
    env = os.environ.copy()
    env["RCLONE_CONFIG"] = "/root/.config/rclone/rclone.conf"
    try:
        result = subprocess.run(
            ["rclone", "lsjson", path, "--dirs-only=false", "--max-depth=1"],
            capture_output=True, text=True, timeout=20, env=env
        )
        if result.returncode == 0:
            import json
            items = json.loads(result.stdout or "[]")
            return jsonify({"ok": True, "items": items, "path": path})
        else:
            return jsonify({"ok": False, "error": result.stderr.strip()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/probe")
def probe_url():
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"ok": False})

    source_type = "direct"
    if any(x in url for x in ["youtube.com", "youtu.be"]):
        source_type = "youtube"
    elif "instagram.com" in url:
        source_type = "instagram"

    def parse_headers(stdout):
        headers = {}
        for line in stdout.splitlines():
            if ":" in line and not line.startswith("HTTP"):
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        return headers

    try:
        # Primary: HEAD with -g -L (handles encoded URLs & redirects)
        result = subprocess.run(
            ["curl", "-sI", "--max-time", "10", "-g", "-L",
             "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
             "-H", "Accept: */*",
             url],
            capture_output=True, text=True, timeout=15
        )
        headers = parse_headers(result.stdout)
        size = None
        raw = headers.get("content-length")
        if raw and raw.isdigit() and int(raw) > 0:
            size = int(raw)
        ctype = headers.get("content-type", "")

        # Fallback: GET Range: bytes=0-0 to get Content-Range total
        if not size:
            result2 = subprocess.run(
                ["curl", "-s", "--max-time", "10", "-g", "-L",
                 "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                 "-H", "Accept: */*",
                 "-H", "Range: bytes=0-0",
                 "-I", url],
                capture_output=True, text=True, timeout=15
            )
            h2 = parse_headers(result2.stdout)
            cr = h2.get("content-range", "")
            if cr and "/" in cr:
                total_str = cr.split("/")[-1].strip()
                if total_str.isdigit() and int(total_str) > 0:
                    size = int(total_str)
            if not size:
                raw2 = h2.get("content-length")
                if raw2 and raw2.isdigit() and int(raw2) > 0:
                    size = int(raw2)
            if not ctype:
                ctype = h2.get("content-type", "")

        return jsonify({"ok": True, "size": size, "content_type": ctype, "source_type": source_type})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
