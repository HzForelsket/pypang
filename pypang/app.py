from __future__ import annotations

import logging
import posixpath
import queue
import tempfile
import threading
import time
import uuid
from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import __version__
from .app_paths import load_available_app_choices
from .client import BaiduPanClient
from .config import AppConfig, runtime_temp_dir
from .errors import ApiError, AuthenticationError, BaiduPanError, ConfigurationError
from .references import BYPY_REFERENCE, OFFICIAL_DOCS
from .storage import StateStore


BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Baidu Pan Web", version=__version__)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
DOWNLOAD_JOBS: dict[str, dict] = {}
DOWNLOAD_JOBS_LOCK = threading.Lock()
logger = logging.getLogger(__name__)


def get_store() -> StateStore:
    return StateStore()


def get_client() -> BaiduPanClient:
    return BaiduPanClient(store=get_store())


def serialize_entry(client: BaiduPanClient, item: dict) -> dict:
    return {
        "fs_id": item.get("fs_id"),
        "name": item.get("server_filename") or Path(str(item.get("path", ""))).name,
        "path": item.get("path"),
        "display_path": client.display_path(str(item.get("path", ""))),
        "is_dir": bool(item.get("isdir")),
        "size": int(item.get("size", 0) or 0),
        "category": int(item.get("category", 0) or 0),
        "server_mtime": int(item.get("server_mtime", 0) or 0),
        "server_ctime": int(item.get("server_ctime", 0) or 0),
        "dir_empty": item.get("dir_empty"),
        "thumbnail": (
            (item.get("thumbs") or {}).get("url2")
            or (item.get("thumbs") or {}).get("url1")
            or (item.get("thumbs") or {}).get("url3")
        ),
    }


def build_breadcrumbs(client: BaiduPanClient, cwd: str) -> list[dict]:
    root = client.config.resolved_app_root()
    breadcrumbs = [{"name": "应用根目录", "path": root or cwd, "display": "/"}]
    if not root or cwd == root:
        return breadcrumbs

    relative = client.display_path(cwd).strip("/")
    if not relative:
        return breadcrumbs

    current = root
    for part in relative.split("/"):
        current = posixpath.join(current, part)
        breadcrumbs.append({"name": part, "path": current, "display": client.display_path(current)})
    return breadcrumbs


def build_local_breadcrumbs(cwd: Path) -> list[dict]:
    resolved = cwd.expanduser().resolve()
    breadcrumbs: list[dict] = []
    current = resolved
    parents = [current, *current.parents]
    for part in reversed(parents):
        name = part.name or str(part)
        breadcrumbs.append({"name": name, "path": str(part)})
    return breadcrumbs


def serialize_local_directory_entry(item: Path) -> dict:
    stat = item.stat()
    return {
        "name": item.name or str(item),
        "path": str(item),
        "display_path": str(item),
        "is_dir": item.is_dir(),
        "size": int(stat.st_size),
        "server_mtime": int(stat.st_mtime),
    }


def normalize_upload_relative_path(raw_path: str) -> str:
    cleaned = str(raw_path or "").replace("\\", "/").strip().lstrip("/")
    if not cleaned:
        return ""
    normalized = posixpath.normpath(cleaned)
    if normalized in {".", ""}:
        return ""
    if normalized.startswith("../") or normalized == "..":
        raise ConfigurationError("Upload relative path is invalid.")
    return normalized


def iter_local_upload_files(source_path: Path) -> list[tuple[Path, str]]:
    source = source_path.expanduser().resolve()
    if not source.exists():
        raise ConfigurationError(f"Local source path does not exist: {source}")
    if source.is_file():
        return [(source, "")]

    jobs: list[tuple[Path, str]] = []
    for child in sorted(source.rglob("*")):
        if not child.is_file():
            continue
        relative = child.relative_to(source).as_posix()
        jobs.append((child, f"{source.name}/{relative}"))
    if not jobs:
        raise ConfigurationError("Selected local folder does not contain any files.")
    return jobs


def bootstrap_payload(client: BaiduPanClient) -> dict:
    state = client.state
    config = state.config
    return {
        "config": config.to_dict(),
        "app_choices": load_available_app_choices(),
        "authorized": client.is_authorized(),
        "ready_for_auth": config.is_ready_for_auth(),
        "ready_for_api": config.is_ready_for_api(),
        "app_root": config.resolved_app_root(),
        "official_docs": OFFICIAL_DOCS,
        "bypy_reference": BYPY_REFERENCE,
    }


def json_error(message: str, *, status_code: int = 400) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status_code)


@app.exception_handler(BaiduPanError)
async def baidu_pan_error_handler(_: Request, exc: BaiduPanError):
    status_code = 401 if isinstance(exc, AuthenticationError) else 400
    return json_error(str(exc), status_code=status_code)


def _build_download_job_plan(
    client: BaiduPanClient, paths: list[str], destination_root: Path
) -> tuple[list[tuple[dict, Path]], int]:
    jobs: list[tuple[dict, Path]] = []
    total_bytes = 0
    for remote_path in paths:
        entry = client.get_entry_by_path(remote_path)
        base_name = Path(str(entry.get("path") or remote_path)).name or "download"
        local_target = destination_root / base_name
        if bool(entry.get("isdir")):
            for child in client._iter_directory_entries(str(entry.get("path") or remote_path)):
                if bool(child.get("isdir")):
                    continue
                relative = child["relative_path"]
                jobs.append((child, local_target.joinpath(*relative.split("/"))))
                total_bytes += int(child.get("size", 0) or 0)
            continue
        jobs.append((entry, local_target))
        total_bytes += int(entry.get("size", 0) or 0)
    return jobs, total_bytes


def _set_download_job(job_id: str, **changes) -> None:
    with DOWNLOAD_JOBS_LOCK:
        job = DOWNLOAD_JOBS.get(job_id)
        if not job:
            return
        job.update(changes)


def _serialize_download_file_state(item: dict) -> dict:
    return {
        "label": item["label"],
        "name": item["name"],
        "path": item["path"],
        "transferred_bytes": int(item.get("transferred_bytes", 0) or 0),
        "total_bytes": int(item.get("total_bytes", 0) or 0),
        "verify_bytes": int(item.get("verify_bytes", 0) or 0),
        "verify_total_bytes": int(item.get("verify_total_bytes", 0) or 0),
        "speed_bps": float(item.get("speed_bps", 0) or 0),
        "status": item.get("status", "waiting"),
    }


def _scan_completed_download_target(
    client: BaiduPanClient,
    entry: dict,
    target: Path,
) -> bool:
    fs_id = int(entry["fs_id"])
    file_name = str(entry.get("server_filename") or Path(str(entry.get("path", ""))).name or "download.bin")
    label = str(entry.get("path") or file_name)
    expected_size = int(entry.get("size", 0) or 0)
    resolved_target = target / file_name if target.exists() and target.is_dir() else target
    partial_target = resolved_target.with_name(f"{resolved_target.name}.part")
    spec = client.build_download_spec(fs_id)

    if resolved_target.exists() and resolved_target.is_file() and expected_size and resolved_target.stat().st_size == expected_size:
        logger.info("Pre-scan verify existing file for %s", label)
        try:
            client._ensure_download_md5(resolved_target, spec=spec, label=label)
        except ApiError:
            logger.warning("Pre-scan remove invalid existing file for %s", label)
            resolved_target.unlink(missing_ok=True)
            partial_target.unlink(missing_ok=True)
            return False
        partial_target.unlink(missing_ok=True)
        return True

    if partial_target.exists() and expected_size and partial_target.stat().st_size == expected_size:
        logger.info("Pre-scan verify completed partial for %s", label)
        try:
            client._ensure_download_md5(partial_target, spec=spec, label=label)
        except ApiError:
            logger.warning("Pre-scan remove invalid partial file for %s", label)
            partial_target.unlink(missing_ok=True)
            return False
        partial_target.replace(resolved_target)
        return True

    return False


def _run_download_job(job_id: str, paths: list[str], destination_path: Path) -> None:
    try:
        client = get_client()
        started_at = time.time()
        download_jobs, total_bytes = _build_download_job_plan(client, paths, destination_path)
        logger.info(
            "Download job %s started: items=%s files=%s destination=%s total_bytes=%s",
            job_id,
            len(paths),
            len(download_jobs),
            destination_path,
            total_bytes,
        )
        waiting_files = [
            {
                "label": str(entry.get("path") or target),
                "name": str(entry.get("server_filename") or Path(str(entry.get("path", ""))).name or target.name),
                "path": str(entry.get("path") or ""),
                "transferred_bytes": 0,
                "total_bytes": int(entry.get("size", 0) or 0),
                "speed_bps": 0.0,
                "status": "waiting",
            }
            for entry, target in download_jobs
        ]
        _set_download_job(
            job_id,
            status="running",
            total_bytes=total_bytes,
            total_files=len(download_jobs),
            started_at=started_at,
            waiting_files=waiting_files,
            active_files=[],
            completed_files=[],
        )
        progress_state = {
            "done": 0,
            "active": {},
            "waiting": {item["label"]: item for item in waiting_files},
            "completed": {},
        }
        progress_lock = threading.Lock()
        worker_error: dict[str, str] = {}

        def sync_job_state(current_label: str = "") -> None:
            with progress_lock:
                active_files = [
                    _serialize_download_file_state(item)
                    for item in sorted(progress_state["active"].values(), key=lambda value: value["name"].lower())
                ]
                waiting_file_list = [
                    _serialize_download_file_state(item)
                    for item in sorted(progress_state["waiting"].values(), key=lambda value: value["name"].lower())
                ]
                completed_files = [
                    _serialize_download_file_state(item)
                    for item in sorted(progress_state["completed"].values(), key=lambda value: value["name"].lower())
                ]
                total_speed = sum(item.get("speed_bps", 0.0) for item in progress_state["active"].values())
                done = progress_state["done"]
            _set_download_job(
                job_id,
                transferred_bytes=done,
                current_file=current_label,
                speed_bps=total_speed,
                active_files=active_files,
                waiting_files=waiting_file_list,
                completed_files=completed_files,
            )

        pending_jobs: list[tuple[dict, Path]] = []
        for entry, target in download_jobs:
            label = str(entry.get("path") or target)
            if _scan_completed_download_target(client, entry, target):
                with progress_lock:
                    current = progress_state["waiting"].pop(label, None)
                    if current:
                        current["status"] = "completed"
                        current["transferred_bytes"] = int(current.get("total_bytes", 0) or 0)
                        current["verify_bytes"] = int(current.get("total_bytes", 0) or 0)
                        current["verify_total_bytes"] = int(current.get("total_bytes", 0) or 0)
                        current["speed_bps"] = 0.0
                        progress_state["completed"][label] = current
                        progress_state["done"] += int(current.get("total_bytes", 0) or 0)
                continue
            pending_jobs.append((entry, target))

        sync_job_state("")
        work_queue: queue.Queue[tuple[dict, Path]] = queue.Queue()
        for job in pending_jobs:
            work_queue.put(job)

        def worker() -> None:
            client = get_client()
            reserved_job: tuple[dict, Path, str] | None = None

            def begin_job(entry: dict, target: Path) -> str:
                current_label = str(entry.get("path") or target)
                file_state = progress_state["waiting"].pop(current_label, None) or {
                    "label": current_label,
                    "name": str(entry.get("server_filename") or Path(current_label).name or target.name),
                    "path": str(entry.get("path") or ""),
                    "transferred_bytes": 0,
                    "total_bytes": int(entry.get("size", 0) or 0),
                    "verify_bytes": 0,
                    "verify_total_bytes": int(entry.get("size", 0) or 0),
                    "speed_bps": 0.0,
                    "status": "downloading",
                }
                file_state.update(
                    {
                        "status": "downloading",
                        "verify_bytes": 0,
                        "verify_total_bytes": int(entry.get("size", 0) or 0),
                        "last_done": int(file_state.get("transferred_bytes", 0) or 0),
                        "last_update_at": time.monotonic(),
                        "initialized": False,
                    }
                )
                progress_state["active"][current_label] = file_state
                return current_label

            while True:
                with progress_lock:
                    if reserved_job is not None:
                        entry, target, label = reserved_job
                        reserved_job = None
                    else:
                        try:
                            entry, target = work_queue.get_nowait()
                        except queue.Empty:
                            return
                        label = begin_job(entry, target)
                sync_job_state(label)
                logger.info("Download job %s start file: %s -> %s", job_id, label, target)
                success = False

                def on_progress(event: dict) -> None:
                    current_label = str(event.get("label") or label)
                    phase = str(event.get("phase") or "downloading")
                    with progress_lock:
                        current = progress_state["active"].get(current_label)
                        if current is None:
                            return
                        current["status"] = phase
                        now = time.monotonic()
                        if phase == "downloading":
                            current["verify_bytes"] = int(event.get("verify_bytes", 0) or 0)
                            current["verify_total_bytes"] = int(
                                event.get("verify_total_bytes", current.get("total_bytes", 0)) or 0
                            )
                            current["total_bytes"] = int(
                                event.get("download_total_bytes", current.get("total_bytes", 0)) or 0
                            )
                            downloaded_bytes = int(event.get("downloaded_bytes", current.get("transferred_bytes", 0)) or 0)
                            delta_bytes = int(event.get("download_delta_bytes", 0) or 0)
                            if delta_bytes < 0:
                                delta_bytes = 0
                            if not current["initialized"] and downloaded_bytes > int(current.get("transferred_bytes", 0) or 0):
                                progress_state["done"] += downloaded_bytes - int(current.get("transferred_bytes", 0) or 0)
                            current["transferred_bytes"] = downloaded_bytes
                            progress_state["done"] += delta_bytes
                            if current["initialized"]:
                                delta_time = max(now - current["last_update_at"], 1e-6)
                                instant_speed = delta_bytes / delta_time if delta_bytes else 0.0
                                if delta_bytes:
                                    current["speed_bps"] = (
                                        current["speed_bps"] * 0.45 + instant_speed * 0.55
                                        if current["speed_bps"] > 0
                                        else instant_speed
                                    )
                                else:
                                    current["speed_bps"] *= 0.6
                            else:
                                current["initialized"] = True
                            current["last_done"] = current["transferred_bytes"]
                        elif phase == "verifying":
                            current["verify_bytes"] = int(event.get("verify_bytes", 0) or 0)
                            current["verify_total_bytes"] = int(
                                event.get("verify_total_bytes", current.get("total_bytes", 0)) or 0
                            )
                            current["speed_bps"] = 0.0
                        elif phase == "completed":
                            completed_bytes = int(
                                event.get("downloaded_bytes", current.get("total_bytes", 0)) or 0
                            )
                            if completed_bytes > int(current.get("transferred_bytes", 0) or 0):
                                progress_state["done"] += completed_bytes - int(current.get("transferred_bytes", 0) or 0)
                            current["transferred_bytes"] = completed_bytes
                            current["verify_bytes"] = int(
                                event.get("verify_bytes", current.get("transferred_bytes", 0)) or 0
                            )
                            current["verify_total_bytes"] = int(
                                event.get("verify_total_bytes", current.get("total_bytes", 0)) or 0
                            )
                            current["speed_bps"] = 0.0
                        current["last_update_at"] = now
                    sync_job_state(current_label)

                try:
                    client._download_entry_to_path(
                        entry,
                        target,
                        resume=True,
                        progress_callback=on_progress,
                    )
                    success = True
                    logger.info("Download job %s finished file: %s", job_id, label)
                except Exception as exc:
                    logger.exception("Download job %s failed on file %s", job_id, label)
                    with progress_lock:
                        if "message" not in worker_error:
                            worker_error["message"] = str(exc)
                    return
                finally:
                    next_label = ""
                    with progress_lock:
                        current = progress_state["active"].pop(label, None)
                        if current and success:
                            current["status"] = "completed"
                            current["speed_bps"] = 0.0
                            progress_state["completed"][label] = current
                            try:
                                next_entry, next_target = work_queue.get_nowait()
                            except queue.Empty:
                                reserved_job = None
                            else:
                                next_label = begin_job(next_entry, next_target)
                                reserved_job = (next_entry, next_target, next_label)
                    sync_job_state(next_label)
                    work_queue.task_done()

        workers = [
            threading.Thread(target=worker, daemon=True)
            for _ in range(min(client.config.effective_web_download_workers(), max(1, len(pending_jobs))))
        ]
        for worker_thread in workers:
            worker_thread.start()
        for worker_thread in workers:
            worker_thread.join()

        if worker_error:
            raise RuntimeError(worker_error["message"])

        _set_download_job(
            job_id,
            status="completed",
            transferred_bytes=total_bytes,
            current_file="",
            speed_bps=0,
            completed_at=time.time(),
            active_files=[],
            waiting_files=[],
            completed_files=[
                _serialize_download_file_state(item)
                for item in sorted(progress_state["completed"].values(), key=lambda value: value["name"].lower())
            ],
            count=len(paths),
        )
        logger.info("Download job %s completed successfully", job_id)
    except Exception as exc:
        logger.exception("Download job %s failed", job_id)
        _set_download_job(
            job_id,
            status="failed",
            error=str(exc),
            speed_bps=0,
            completed_at=time.time(),
        )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    client = get_client()
    return templates.TemplateResponse(
        name="index.html",
        request=request,
        context={
            "bootstrap": bootstrap_payload(client),
        },
    )


@app.get("/api/status")
async def api_status():
    client = get_client()
    payload = bootstrap_payload(client)
    payload["has_token"] = bool(client.token and client.token.access_token)
    return payload


@app.post("/api/settings")
async def api_settings(request: Request):
    store = get_store()
    state = store.load()
    current = state.config.to_dict()
    incoming = await request.json()

    for key in current.keys():
        if key in incoming and incoming[key] is not None:
            current[key] = incoming[key]

    updated = AppConfig.from_dict(current)
    store.update_config(updated, clear_token=False)
    client = get_client()
    return bootstrap_payload(client)


@app.post("/api/logout")
async def api_logout():
    get_store().clear_token()
    return {"ok": True}


@app.post("/api/refresh-token")
async def api_refresh_token():
    token = get_client().refresh_access_token()
    return token.to_dict()


@app.post("/api/exchange-code")
async def api_exchange_code(request: Request):
    payload = await request.json()
    code = str(payload.get("code", "")).strip()
    if not code:
        raise ConfigurationError("Authorization code is required.")
    token = get_client().exchange_code(code)
    return token.to_dict()


@app.get("/auth/login")
async def auth_login():
    client = get_client()
    return RedirectResponse(client.build_authorize_url(), status_code=302)


@app.get("/auth/callback", response_class=HTMLResponse)
async def auth_callback(request: Request, code: str | None = None, error: str | None = None):
    if error:
        return templates.TemplateResponse(
            name="callback.html",
            request=request,
            context={"success": False, "message": f"百度授权失败: {error}"},
        )
    if not code:
        return templates.TemplateResponse(
            name="callback.html",
            request=request,
            context={"success": False, "message": "回调里没有收到 code。"},
        )
    try:
        get_client().exchange_code(code)
    except BaiduPanError as exc:
        return templates.TemplateResponse(
            name="callback.html",
            request=request,
            context={"success": False, "message": str(exc)},
        )
    return templates.TemplateResponse(
        name="callback.html",
        request=request,
        context={"success": True, "message": "授权完成，页面会自动跳回控制台。"},
    )


@app.get("/api/profile")
async def api_profile():
    return get_client().get_user_info()


@app.get("/api/quota")
async def api_quota():
    return get_client().get_quota()


@app.get("/api/files")
async def api_files(dir: str = "/"):
    client = get_client()
    listing = client.list_files(dir)
    cwd = str(listing.get("cwd") or client.normalize_remote_path(dir))
    entries = [serialize_entry(client, item) for item in listing.get("list", [])]
    entries.sort(key=lambda item: (not item["is_dir"], item["name"].lower()))
    return {
        "cwd": {
            "path": cwd,
            "display": client.display_path(cwd),
        },
        "breadcrumbs": build_breadcrumbs(client, cwd),
        "entries": entries,
        "app_root": client.config.resolved_app_root(),
    }


@app.get("/api/server-paths")
async def api_server_paths(dir: str = "./downloads", include_files: bool = False):
    cwd = Path(dir).expanduser().resolve()
    if cwd.exists() and not cwd.is_dir():
        raise ConfigurationError("Server path must be a directory.")
    if not cwd.exists():
        cwd.mkdir(parents=True, exist_ok=True)

    entries = sorted(
        [
            serialize_local_directory_entry(item)
            for item in cwd.iterdir()
            if include_files or item.is_dir()
        ],
        key=lambda item: (not item["is_dir"], item["name"].lower()),
    )
    return {
        "cwd": {"path": str(cwd), "name": cwd.name or str(cwd)},
        "breadcrumbs": build_local_breadcrumbs(cwd),
        "entries": entries,
    }


@app.post("/api/folders")
async def api_folders(request: Request):
    payload = await request.json()
    path = payload.get("path")
    rename_on_conflict = bool(payload.get("rename_on_conflict"))
    if not path:
        raise ConfigurationError("Folder path is required.")
    return get_client().create_folder(path, rename_on_conflict=rename_on_conflict)


@app.post("/api/rename")
async def api_rename(request: Request):
    payload = await request.json()
    return get_client().rename(payload["path"], payload["new_name"])


@app.post("/api/move")
async def api_move(request: Request):
    payload = await request.json()
    return get_client().move(
        payload["path"],
        payload["destination_dir"],
        new_name=payload.get("new_name"),
    )


@app.post("/api/delete")
async def api_delete(request: Request):
    payload = await request.json()
    paths = payload.get("paths") or []
    if not paths:
        raise ConfigurationError("At least one path is required.")
    return get_client().delete(paths)


@app.post("/api/download-to-server")
async def api_download_to_server(request: Request):
    payload = await request.json()
    paths = payload.get("paths") or []
    destination = str(payload.get("destination", "")).strip()
    if not paths:
        raise ConfigurationError("At least one remote path is required.")
    if not destination:
        raise ConfigurationError("Server destination is required.")

    client = get_client()
    destination_path = Path(destination).expanduser().resolve()
    preview_jobs, total_bytes = _build_download_job_plan(client, paths, destination_path)
    job_id = uuid.uuid4().hex
    with DOWNLOAD_JOBS_LOCK:
        DOWNLOAD_JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "destination": str(destination_path),
            "count": len(paths),
            "total_files": len(preview_jobs),
            "total_bytes": total_bytes,
            "transferred_bytes": 0,
            "speed_bps": 0,
            "current_file": "",
            "active_files": [],
            "waiting_files": [],
            "completed_files": [],
            "error": "",
            "created_at": time.time(),
        }
    worker = threading.Thread(
        target=_run_download_job,
        args=(job_id, list(paths), destination_path),
        daemon=True,
    )
    worker.start()
    return {
        "job_id": job_id,
        "destination": str(destination_path),
        "count": len(paths),
        "total_files": len(preview_jobs),
        "total_bytes": total_bytes,
    }


@app.get("/api/download-to-server/{job_id}")
async def api_download_to_server_status(job_id: str):
    with DOWNLOAD_JOBS_LOCK:
        job = DOWNLOAD_JOBS.get(job_id)
        if not job:
            raise ConfigurationError("Download job not found.")
        return dict(job)


@app.post("/api/upload")
async def api_upload(
    target_dir: str = Form("/"),
    target_path: str = Form(""),
    target_kind: str = Form("dir"),
    policy: str = Form("overwrite"),
    relative_paths: list[str] = Form([]),
    files: list[UploadFile] = File(...),
):
    client = get_client()
    temp_root = runtime_temp_dir()
    temp_root.mkdir(parents=True, exist_ok=True)
    uploaded = []
    normalized_target_kind = (target_kind or "dir").strip().lower()
    if normalized_target_kind not in {"dir", "file"}:
        raise ConfigurationError("Upload target kind must be dir or file.")
    relative_path_list = list(relative_paths or [])
    if relative_path_list and len(relative_path_list) != len(files):
        raise ConfigurationError("Upload relative paths do not match file count.")

    if normalized_target_kind == "file" and (len(files) > 1 or any(path.strip() for path in relative_path_list)):
        raise ConfigurationError("Uploading folders or multiple files requires selecting a server directory target.")

    for index, file in enumerate(files):
        file_name = Path(file.filename or "upload.bin").name
        suffix = Path(file_name).suffix
        with tempfile.NamedTemporaryFile(delete=False, dir=temp_root, suffix=suffix) as handle:
            temp_path = Path(handle.name)
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)

        remote_input = target_path.strip() or target_dir.strip() or "/"
        relative_path = normalize_upload_relative_path(relative_path_list[index] if index < len(relative_path_list) else "")
        try:
            if relative_path:
                remote_root = client.normalize_remote_path(remote_input)
                relative_parts = [part for part in relative_path.split("/") if part]
                if not relative_parts:
                    raise ConfigurationError("Upload relative path is invalid.")
                parent_parts = relative_parts[:-1]
                if parent_parts:
                    parent_dir = client.ensure_remote_directory(posixpath.join(remote_root, *parent_parts))
                else:
                    parent_dir = remote_root
                resolved_remote_path = client.resolve_upload_target(relative_parts[-1], parent_dir)
            else:
                resolved_remote_path = client.resolve_upload_target(file_name, remote_input)
            result = client.upload_file(temp_path, resolved_remote_path, policy=policy)
            uploaded.append(
                {
                    "name": file_name,
                    "relative_path": relative_path,
                    "remote_path": resolved_remote_path,
                    "result": result,
                }
            )
        finally:
            temp_path.unlink(missing_ok=True)

    return {"uploaded": uploaded}


@app.post("/api/upload-server-source")
async def api_upload_server_source(request: Request):
    payload = await request.json()
    source_path = str(payload.get("source_path", "")).strip()
    target_dir = str(payload.get("target_dir", "")).strip() or "/"
    target_path = str(payload.get("target_path", "")).strip()
    policy = str(payload.get("policy", "overwrite")).strip() or "overwrite"
    if not source_path:
        raise ConfigurationError("Server source path is required.")

    client = get_client()
    remote_input = target_path or target_dir or "/"
    remote_root = client.ensure_remote_directory(remote_input)
    uploaded = []
    for file_path, relative_path in iter_local_upload_files(Path(source_path)):
        if relative_path:
            relative_parts = [part for part in normalize_upload_relative_path(relative_path).split("/") if part]
            parent_parts = relative_parts[:-1]
            parent_dir = remote_root
            if parent_parts:
                parent_dir = client.ensure_remote_directory(posixpath.join(remote_root, *parent_parts))
            resolved_remote_path = client.resolve_upload_target(relative_parts[-1], parent_dir)
        else:
            resolved_remote_path = client.resolve_upload_target(file_path.name, remote_root)
        result = client.upload_file(file_path, resolved_remote_path, policy=policy)
        uploaded.append(
            {
                "name": file_path.name,
                "relative_path": relative_path,
                "remote_path": resolved_remote_path,
                "result": result,
            }
        )
    return {"uploaded": uploaded}


@app.get("/api/download")
async def api_download(request: Request, fs_id: int | None = None, path: str | None = None):
    client = get_client()
    if path:
        entry = client.get_entry_by_path(path)
        if bool(entry.get("isdir")):
            raise ConfigurationError("Directory streaming is handled by the Web client. Please choose a local folder in the browser first.")
        spec, response = client.open_download(int(entry["fs_id"]), byte_range=request.headers.get("range"))
    elif fs_id is not None:
        spec, response = client.open_download(fs_id, byte_range=request.headers.get("range"))
    else:
        raise ConfigurationError("Either fs_id or path is required.")

    def iterator():
        try:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    yield chunk
        finally:
            response.close()

    headers = {
        "Content-Disposition": f'attachment; filename="{spec.file_name}"',
        "Accept-Ranges": response.headers.get("Accept-Ranges", "bytes"),
    }
    if response.headers.get("Content-Length"):
        headers["Content-Length"] = response.headers["Content-Length"]
    if response.headers.get("Content-Range"):
        headers["Content-Range"] = response.headers["Content-Range"]

    media_type = response.headers.get("Content-Type", "application/octet-stream")
    return StreamingResponse(
        iterator(),
        media_type=media_type,
        headers=headers,
        status_code=response.status_code,
    )
