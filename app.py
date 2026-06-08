from flask import Flask, request, render_template, jsonify
import uuid
from worker import job_queue, jobs, jobs_lock, processes, processes_lock

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start():
    url      = request.form.get("url", "").strip()
    filename = request.form.get("filename", "").strip()

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
            "progress":    0,
            "retries":     0,
            "started_at":  None,
            "finished_at": None,
        }

    job_queue.put({"id": job_id, "url": url, "filename": filename})
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
    # Cancel first if running
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
    # Kill all running processes
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
