import io
import json
import os
import time
import uuid
from functools import wraps

from flask import (
    Flask,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from main import dedup_by_email, filter_no_email, generate_csvs, load_file_objects

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

# In-memory job store: {job_id: {"meta": {...}, "files": {filename: bytes}, "ts": float}}
JOBS: dict = {}
JOB_TTL = 3600  # seconds before a job is eligible for cleanup

CATEGORIES = [
    ("agent", "Agent"),
    ("presenter", "Presenter"),
    ("artist", "Artist"),
    ("record_label", "Record Label"),
]


def _evict_old_jobs():
    cutoff = time.time() - JOB_TTL
    stale = [jid for jid, j in JOBS.items() if j["ts"] < cutoff]
    for jid in stale:
        del JOBS[jid]


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authed"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == APP_PASSWORD:
            session["authed"] = True
            return redirect(url_for("index"))
        error = "Incorrect password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/process", methods=["POST"])
@login_required
def process():
    _evict_old_jobs()

    job_id = str(uuid.uuid4())
    counts = {}
    skipped = {}
    files_by_category = {}
    file_bytes = {}

    for key, label in CATEGORIES:
        uploads = request.files.getlist(f"{key}_files")
        uploads = [f for f in uploads if f and f.filename and f.filename.lower().endswith(".xlsx")]
        if not uploads:
            continue
        raw = load_file_objects(uploads)
        df, no_email_count = filter_no_email(raw)
        df = dedup_by_email(df)
        counts[label] = len(df)
        if no_email_count:
            skipped[label] = no_email_count
        csvs = generate_csvs(key, df)
        for filename, (data, _mime) in csvs.items():
            file_bytes[filename] = data
        files_by_category[label] = list(csvs.keys())

    JOBS[job_id] = {
        "meta": {"counts": counts, "skipped": skipped, "files": files_by_category},
        "files": file_bytes,
        "ts": time.time(),
    }

    return redirect(url_for("results", job_id=job_id))


@app.route("/results/<job_id>")
@login_required
def results(job_id):
    job = JOBS.get(job_id)
    if not job:
        return render_template("expired.html"), 404
    meta = job["meta"]
    total = sum(meta["counts"].values())
    return render_template("results.html", job_id=job_id, meta=meta, total=total)


@app.route("/download/<job_id>/<filename>")
@login_required
def download(job_id, filename):
    job = JOBS.get(job_id)
    if not job or filename not in job["files"]:
        return render_template("expired.html"), 404
    data = job["files"][filename]
    return send_file(io.BytesIO(data), as_attachment=True, download_name=filename)


if __name__ == "__main__":
    app.run(debug=True)
