"""
WebtapuApp - Flask application for PDF processing
Simple web interface for uploading and processing Turkish property documents
"""
import os
import json
import tempfile
import threading
import uuid
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Dict

from flask import Flask, render_template, request, send_file, flash, redirect, url_for, jsonify, Response
from werkzeug.utils import secure_filename

from pdf_processor import PDFProcessor, process_pdf_files

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key')

# Configuration
UPLOAD_FOLDER = Path(tempfile.gettempdir()) / 'webtapu_uploads'
ALLOWED_EXTENSIONS = {'pdf'}
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# In-memory job registry for progress tracking
jobs: Dict[str, Dict[str, Any]] = {}
jobs_lock = threading.Lock()
SUPPORTED_OUTPUT_FORMATS = {"excel", "csv"}

# Ensure upload directory exists
UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)


def allowed_file(filename):
    """Check if file has allowed extension."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def create_job(output_format: str, total_files: int) -> str:
    """Create a new processing job and register it in memory."""
    job_id = uuid.uuid4().hex
    event_queue: Queue = Queue()
    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "output_format": output_format,
            "total": total_files,
            "processed": 0,
            "queue": event_queue,
            "output_path": None,
            "error": None,
        }
    return job_id


def emit_job_event(job_id: str, event: Dict[str, Any]) -> None:
    """Push an event to the job queue and update metadata."""
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        status = event.get("event")
        if status == "progress":
            job["status"] = "processing"
            job["processed"] = event.get("current", job.get("processed", 0))
        elif status == "complete":
            job["status"] = "finalizing"
        elif status == "error":
            job["status"] = "error"
            job["error"] = event.get("message")
        elif status in {"output_ready", "finished"}:
            job["status"] = "completed"
        job["queue"].put(event)


from typing import Any, Dict, Optional

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a job by ID."""
    with jobs_lock:
        return jobs.get(job_id)


def process_job(job_id: str, pdf_paths: list[Path], output_format: str, clean_watermarks: bool) -> None:
    """Background worker that processes PDF files and streams progress."""
    output_extension = "xlsx" if output_format == "excel" else "csv"
    output_dir = app.config['UPLOAD_FOLDER'] / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{job_id}.{output_extension}"

    def progress_callback(event: Dict[str, Any]) -> None:
        emit_job_event(job_id, event)

    try:
        result = process_pdf_files(
            pdf_paths,
            output_format=output_format,
            output_path=output_path,
            clean_watermarks=clean_watermarks,
            progress_callback=progress_callback,
        )

        if result.get("success"):
            with jobs_lock:
                job = jobs.get(job_id)
                if job is not None:
                    job["output_path"] = str(result["output_path"])
                    job["status"] = "completed"
                    download_name = job.get("download_name")
                else:
                    download_name = None
            with app.test_request_context():
                emit_job_event(job_id, {
                    "event": "finished",
                    "message": result["message"],
                    "download_url": url_for('download_output', job_id=job_id),
                    "download_name": download_name,
                })
        else:
            emit_job_event(job_id, {
                "event": "error",
                "message": result.get("message", "Processing failed")
            })
    except Exception as exc:
        emit_job_event(job_id, {
            "event": "error",
            "message": f"Unexpected error: {exc}"
        })
    finally:
        # Clean up uploaded PDF files
        for path in pdf_paths:
            try:
                os.remove(path)
            except OSError as e:
                app.logger.error(f"Error deleting file {path}: {e}")


@app.route('/')
def index():
    """Main page with file upload form."""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle PDF file uploads and processing."""
    if 'pdf_files' not in request.files:
        flash('No files selected', 'error')
        return redirect(request.url)
    
    files = request.files.getlist('pdf_files')
    output_format = request.form.get('output_format', 'excel')
    clean_watermarks = True  # Watermark removal is now mandatory
    
    # Filter valid PDF files
    pdf_files = []
    for file in files:
        if file and file.filename and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = UPLOAD_FOLDER / filename
            file.save(file_path)
            pdf_files.append(file_path)
    
    if not pdf_files:
        flash('No valid PDF files uploaded', 'error')
        return redirect(url_for('index'))
    
    # Process PDF files
    try:
        result = process_pdf_files(
            pdf_files, 
            output_format=output_format,
            clean_watermarks=clean_watermarks
        )
        
        if result['success']:
            output_path = result['output_path']
            
            # Generate download filename
            if len(pdf_files) == 1:
                base_name = Path(pdf_files[0]).stem
            else:
                base_name = f"processed_{len(pdf_files)}_files"
            
            download_name = f"{base_name}.{output_format}"
            
            flash(f'Successfully processed {len(pdf_files)} PDF files', 'success')
            return send_file(
                output_path,
                as_attachment=True,
                download_name=download_name,
                mimetype=f'application/{output_format}'
            )
        else:
            flash(f'Processing failed: {result["message"]}', 'error')
            return redirect(url_for('index'))
            
    except Exception as e:
        flash(f'Error during processing: {str(e)}', 'error')
        return redirect(url_for('index'))
    finally:
        # Clean up uploaded PDF files
        for file_path in pdf_files:
            try:
                os.remove(file_path)
            except OSError as e:
                app.logger.error(f"Error deleting file {file_path}: {e}")


@app.route('/api/process', methods=['POST'])
def api_process():
    """API endpoint to handle PDF uploads and trigger asynchronous processing."""
    if 'pdf_files' not in request.files:
        return jsonify({"success": False, "message": "No files selected"}), 400

    files = request.files.getlist('pdf_files')
    output_format = request.form.get('output_format', 'excel').lower()

    if output_format not in SUPPORTED_OUTPUT_FORMATS:
        return jsonify({"success": False, "message": f"Unsupported output format: {output_format}"}), 400

    valid_files = []
    for file in files:
        if file and file.filename and allowed_file(file.filename):
            valid_files.append(file)

    if not valid_files:
        return jsonify({"success": False, "message": "No valid PDF files uploaded"}), 400

    job_id = create_job(output_format, len(valid_files))
    job_upload_dir = app.config['UPLOAD_FOLDER'] / job_id
    job_upload_dir.mkdir(parents=True, exist_ok=True)

    pdf_paths = []
    for file in valid_files:
        filename = secure_filename(file.filename)
        file_path = job_upload_dir / filename
        file.save(file_path)
        pdf_paths.append(file_path)

    if len(pdf_paths) == 1:
        base_name = pdf_paths[0].stem
    else:
        base_name = f"processed_{len(pdf_paths)}_files"

    extension = "xlsx" if output_format == "excel" else "csv"
    download_name = f"{base_name}.{extension}"

    with jobs_lock:
        job = jobs.get(job_id)
        if job is not None:
            job["files"] = [str(path) for path in pdf_paths]
            job["download_name"] = download_name

    emit_job_event(job_id, {
        "event": "queued",
        "message": f"Job queued with {len(pdf_paths)} PDF file(s)"
    })

    worker = threading.Thread(
        target=process_job,
        args=(job_id, pdf_paths, output_format, True),
        daemon=True,
    )
    worker.start()

    return jsonify({"success": True, "job_id": job_id})


@app.route('/api/progress/<job_id>')
def api_progress(job_id):
    """Stream Server-Sent Events for job progress."""
    job = get_job(job_id)
    if job is None:
        return jsonify({"success": False, "message": "Job not found"}), 404

    def event_stream():
        queue: Queue = job["queue"]
        # Send initial handshake event
        yield f"event: connected\ndata: {json.dumps({'job_id': job_id})}\n\n"
        while True:
            try:
                event = queue.get(timeout=15)
            except Empty:
                # Keep-alive comment to prevent timeouts
                yield ": heartbeat\n\n"
                continue

            event_name = event.get("event", "message")
            yield f"event: {event_name}\n"
            yield f"data: {json.dumps(event)}\n\n"

            if event_name in {"finished", "error"}:
                break

    response = Response(event_stream(), mimetype='text/event-stream')
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


@app.route('/api/download/<job_id>')
def download_output(job_id):
    """Provide the processed output file for download."""
    job = get_job(job_id)
    if job is None or not job.get("output_path"):
        flash("Üzgünüz, bu iş için indirilebilir bir dosya bulunamadı.", "error")
        return redirect(url_for('index'))

    output_path = Path(job["output_path"])
    if not output_path.exists():
        flash("Çıktı dosyası bulunamadı.", "error")
        return redirect(url_for('index'))

    download_name = job.get("download_name", output_path.name)
    return send_file(
        output_path,
        as_attachment=True,
        download_name=download_name,
        mimetype=f"application/{output_path.suffix.lstrip('.')}"
    )


@app.route('/about')
def about():
    """About page with application information."""
    return render_template('about.html')


@app.errorhandler(413)
def too_large(e):
    """Handle file too large error."""
    flash('File too large. Maximum size is 100MB.', 'error')
    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
