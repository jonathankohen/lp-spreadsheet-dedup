import json
import os
import uuid
from functools import wraps
from pathlib import Path

from flask import (
    Flask,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from main import dedup_by_email, generate_csvs, load_file_objects

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
JOBS_DIR = Path("/tmp/pollstar_jobs")
JOBS_DIR.mkdir(exist_ok=True)

CATEGORIES = [
    ("agent", "Agent"),
    ("presenter", "Presenter"),
    ("artist", "Artist"),
    ("record_label", "Record Label"),
]


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
    job_id = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir()

    counts = {}
    files_by_category = {}

    for key, label in CATEGORIES:
        uploads = request.files.getlist(f"{key}_files")
        uploads = [f for f in uploads if f and f.filename and f.filename.lower().endswith(".xlsx")]
        if not uploads:
            continue
        df = dedup_by_email(load_file_objects(uploads))
        count = len(df)
        counts[label] = count
        csvs = generate_csvs(key, df)
        for filename, (data, _mime) in csvs.items():
            (job_dir / filename).write_bytes(data)
        files_by_category[label] = list(csvs.keys())

    meta = {"counts": counts, "files": files_by_category}
    (job_dir / "meta.json").write_text(json.dumps(meta))

    return redirect(url_for("results", job_id=job_id))


@app.route("/results/<job_id>")
@login_required
def results(job_id):
    job_dir = JOBS_DIR / job_id
    if not job_dir.exists():
        return "Job not found.", 404
    meta = json.loads((job_dir / "meta.json").read_text())
    total = sum(meta["counts"].values())
    return render_template("results.html", job_id=job_id, meta=meta, total=total)


@app.route("/download/<job_id>/<filename>")
@login_required
def download(job_id, filename):
    job_dir = JOBS_DIR / job_id
    filepath = job_dir / filename
    if not filepath.exists() or not filepath.resolve().is_relative_to(job_dir.resolve()):
        return "File not found.", 404
    return send_file(filepath, as_attachment=True, download_name=filename)


if __name__ == "__main__":
    app.run(debug=True)
