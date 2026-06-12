from flask import Flask, request, render_template, jsonify
import uuid
import subprocess
import shlex
from worker import job_queue, jobs, jobs_lock, processes, processes_lock, session_stats, stats_lock
import logins

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/logins", methods=["GET"])
def logins_list():
    return jsonify({"logins": logins.list_logins()})


@app.route("/logins/add", methods=["POST"])
def logins_add():
    login_url = request.form.get("login_url", "").strip()
    username  = request.form.get("username", "").strip()
    password  = request.form.get("password", "").strip()
    user_field = request.form.get("user_field", "").strip()
    pass_field = request.form.get("pass_field", "").strip()

    if not login_url or not username or not password:
        return jsonify({"ok": False, "error": "login_url, username, password required"}), 400
    if not login_url.startswith("http://") and not login_url.startswith("https://"):
        return jsonify({"ok": False, "error": "login_url must start with http:// or https://"}), 400

    result = logins.perform_login(login_url, username, password,
                                    user_field or None, pass_field or None)
    return jsonify({"ok": True, **result})


@app.route("/logins/remove", methods=["POST"])
def logins_remove():
    domain = request.form.get("domain", "").strip()
    if not domain:
        return jsonify({"ok": False, "error": "domain required"}), 400
    removed = logins.remove_login(domain)
    return jsonify({"ok": removed})


@app.route("/logins/detect", methods=["GET"])
def logins_detect():
    login_url = request.args.get("login_url", "").strip()
    if not login_url:
        return jsonify({"ok": False, "error": "login_url required"}), 400
    user_field, pass_field = logins.detect_login_fields(login_url)
    return jsonify({"ok": True, "user_field": user_field, "pass_field": pass_field})


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
    try:
        result = subprocess.run(
            ["curl", "-sI", "--max-time", "10", "-L",
             "-H", "User-Agent: Mozilla/5.0",
             url],
            capture_output=True, text=True, timeout=15
        )
        headers = {}
        for line in result.stdout.splitlines():
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()

        size = None
        raw = headers.get("content-length")
        if raw and raw.isdigit():
            size = int(raw)

        ctype = headers.get("content-type", "")

        # detect yt-dlp sources
        source_type = "direct"
        if any(x in url for x in ["youtube.com", "youtu.be"]):
            source_type = "youtube"
        elif "instagram.com" in url:
            source_type = "instagram"

        return jsonify({"ok": True, "size": size, "content_type": ctype, "source_type": source_type})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
