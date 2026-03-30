from __future__ import annotations

import hashlib
import io
import json
import logging
import math
import posixpath
import re
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

import requests

from .config import AppConfig
from .errors import ApiError, AuthenticationError, BaiduPanError, ConfigurationError
from .storage import OAuthToken, StateStore


PAN_BASE_URL = "https://pan.baidu.com"
PCS_BASE_URL = "https://d.pcs.baidu.com"
OPENAPI_BASE_URL = "https://openapi.baidu.com"
NETDISK_APP_ID = 250528
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024
FIRST_SLICE_SIZE = 256 * 1024
DEFAULT_DOWNLOAD_WORKERS = 4
DEFAULT_SINGLE_FILE_DOWNLOAD_WORKERS = 4
MIN_SINGLE_FILE_PARALLEL_SIZE = 8 * 1024 * 1024
CHECKSUM_READ_SIZE = 8 * 1024 * 1024
DEFAULT_VOLUME_SPLIT_SIZE = 2_000_000_000
DEFAULT_UPLOAD_VOLUME_WORKERS = 4
DEFAULT_API_TIMEOUT = 120
DEFAULT_UPLOAD_TIMEOUT = 180
DEFAULT_UPLOAD_RETRIES = 3
DEFAULT_UPLOAD_RETRY_BACKOFF = 2.0
logger = logging.getLogger(__name__)
MD5_RE = re.compile(r"^[0-9a-f]{32}$")


@dataclass(slots=True)
class UploadDigestPlan:
    size: int
    content_md5: str
    slice_md5: str
    block_list: list[str]
    local_ctime: int
    local_mtime: int


@dataclass(slots=True)
class DownloadSpec:
    file_name: str
    file_path: str
    dlink: str
    size: int = 0
    md5: str = ""


@dataclass(slots=True)
class UploadVolumeSpec:
    index: int
    start: int
    size: int
    target: str
    digest_plan: UploadDigestPlan | None = None
    existing_entry: dict[str, Any] | None = None


class _MultiVolumeUploadProgress:
    def __init__(self, callback: UploadProgressCallback | None, *, label: str, total_bytes: int, volume_count: int):
        self._callback = callback
        self._label = label
        self._total_bytes = int(total_bytes)
        self._volume_count = int(volume_count)
        self._lock = threading.Lock()
        self._volume_progress: dict[int, int] = {}
        self._completed_volumes: set[int] = set()

    def callback_for(self, volume_index: int) -> UploadProgressCallback | None:
        if self._callback is None:
            return None

        def _callback(event: dict[str, Any]) -> None:
            phase = str(event.get("phase") or "")
            transferred = int(event.get("transferred_bytes", 0) or 0)
            total = int(event.get("total_bytes", 0) or 0)
            delta = int(event.get("delta_bytes", 0) or 0)
            with self._lock:
                self._volume_progress[int(volume_index)] = max(0, min(transferred, total or transferred))
                if phase == "completed":
                    self._completed_volumes.add(int(volume_index))
                aggregate_transferred = sum(self._volume_progress.values())
                active_uploads = sum(
                    1
                    for index, value in self._volume_progress.items()
                    if index not in self._completed_volumes and value > 0
                )
            self._callback(
                {
                    "phase": "completed"
                    if len(self._completed_volumes) >= self._volume_count
                    else "uploading",
                    "label": self._label,
                    "transferred_bytes": aggregate_transferred,
                    "total_bytes": self._total_bytes,
                    "delta_bytes": delta,
                    "active_uploads": int(max(1, active_uploads) if aggregate_transferred > 0 else active_uploads),
                    "completed_volumes": int(len(self._completed_volumes)),
                    "volume_count": self._volume_count,
                }
            )

        return _callback


DownloadProgressCallback = Callable[[dict[str, Any]], None]
UploadProgressCallback = Callable[[dict[str, Any]], None]


class _ProgressReader:
    def __init__(
        self,
        raw,
        *,
        callback: Callable[[int], None] | None = None,
    ):
        self._raw = raw
        self._callback = callback

    def read(self, size: int = -1):
        chunk = self._raw.read(size)
        if chunk and self._callback:
            self._callback(len(chunk))
        return chunk

    def __getattr__(self, name: str):
        return getattr(self._raw, name)


class _RangeReader:
    def __init__(self, raw, *, start: int = 0, length: int | None = None):
        self._raw = raw
        self._start = max(0, int(start))
        self._length = None if length is None else max(0, int(length))
        self._position = 0
        self._raw.seek(self._start)

    def read(self, size: int = -1):
        if self._length is not None:
            remaining = self._length - self._position
            if remaining <= 0:
                return b""
            if size is None or size < 0 or size > remaining:
                size = remaining
        chunk = self._raw.read(size)
        self._position += len(chunk)
        return chunk

    def __getattr__(self, name: str):
        return getattr(self._raw, name)


class BaiduPanClient:
    def __init__(
        self,
        *,
        store: StateStore | None = None,
        session: requests.Session | None = None,
    ):
        self.store = store or StateStore()
        self.session = session or requests.Session()
        self._cached_profile: dict[str, Any] | None = None

    @property
    def state(self):
        return self.store.load()

    @property
    def config(self) -> AppConfig:
        return self.state.config

    @property
    def token(self) -> OAuthToken | None:
        return self.state.token

    def upload_chunk_size(self) -> int:
        chunk_mb = self.effective_upload_chunk_mb()
        return max(1, int(chunk_mb)) * 1024 * 1024

    def download_worker_count(self) -> int:
        workers = getattr(self.config, "effective_cli_download_workers", lambda: DEFAULT_DOWNLOAD_WORKERS)()
        return max(1, int(workers))

    def single_file_download_worker_count(self) -> int:
        workers = getattr(
            self.config,
            "effective_single_file_download_workers",
            lambda: DEFAULT_SINGLE_FILE_DOWNLOAD_WORKERS,
        )()
        return max(1, int(workers))

    def account_membership_tier(self) -> str:
        if not self.is_authorized():
            return self.config.normalized_membership_tier()
        if self._cached_profile is None:
            try:
                self._cached_profile = self.get_user_info()
            except BaiduPanError:
                return self.config.normalized_membership_tier()
        vip_type = int((self._cached_profile or {}).get("vip_type", 0) or 0)
        if vip_type == 2:
            return "svip"
        if vip_type == 1:
            return "vip"
        return "free"

    def effective_membership_tier(self) -> str:
        configured = self.config.normalized_membership_tier()
        detected = self.account_membership_tier()
        order = {"free": 0, "vip": 1, "svip": 2}
        return detected if order.get(detected, 0) >= order.get(configured, 0) else configured

    def effective_upload_chunk_mb(self) -> int:
        tier = self.effective_membership_tier()
        if tier == "svip":
            maximum = 32
        elif tier == "vip":
            maximum = 16
        else:
            maximum = 4
        configured = max(0, int(self.config.upload_chunk_mb or 0))
        if configured <= 0:
            return maximum
        return max(1, min(configured, maximum))

    def max_upload_file_bytes(self) -> int:
        tier = self.effective_membership_tier()
        if tier == "svip":
            return 20_000_000_000
        if tier == "vip":
            return 10_000_000_000
        return 4_000_000_000

    def volume_split_bytes(self) -> int:
        return min(self.max_upload_file_bytes(), DEFAULT_VOLUME_SPLIT_SIZE)

    def is_authorized(self) -> bool:
        token = self.token
        return bool(token and token.access_token)

    def build_authorize_url(self, *, state: str | None = None) -> str:
        config = self.config
        if not config.is_ready_for_auth():
            raise ConfigurationError(
                "Please configure app_key, secret_key, and redirect_uri first."
            )
        params = {
            "response_type": "code",
            "client_id": config.app_key,
            "redirect_uri": config.redirect_uri,
            "scope": config.scope,
        }
        if config.app_id:
            params["device_id"] = config.app_id
        if state:
            params["state"] = state
        request = requests.Request(
            "GET", f"{OPENAPI_BASE_URL}/oauth/2.0/authorize", params=params
        )
        prepared = request.prepare()
        if not prepared.url:
            raise ConfigurationError("Failed to build the authorization URL.")
        return prepared.url

    def exchange_code(self, code: str) -> OAuthToken:
        config = self.config
        if not config.is_ready_for_auth():
            raise ConfigurationError(
                "Please configure app_key, secret_key, and redirect_uri first."
            )
        payload = self._request_json(
            "GET",
            f"{OPENAPI_BASE_URL}/oauth/2.0/token",
            params={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": config.app_key,
                "client_secret": config.secret_key,
                "redirect_uri": config.redirect_uri,
            },
            auth_request=True,
        )
        token = OAuthToken.from_oauth_payload(payload)
        self.store.update_token(token)
        return token

    def refresh_access_token(self) -> OAuthToken:
        token = self.token
        config = self.config
        if not token or not token.refresh_token:
            raise AuthenticationError("No refresh token is available.")
        payload = self._request_json(
            "GET",
            f"{OPENAPI_BASE_URL}/oauth/2.0/token",
            params={
                "grant_type": "refresh_token",
                "refresh_token": token.refresh_token,
                "client_id": config.app_key,
                "client_secret": config.secret_key,
            },
            auth_request=True,
        )
        refreshed = OAuthToken.from_oauth_payload(payload)
        self.store.update_token(refreshed)
        return refreshed

    def get_user_info(self) -> dict[str, Any]:
        payload = self._request_json(
            "GET",
            f"{PAN_BASE_URL}/rest/2.0/xpan/nas",
            params={
                "method": "uinfo",
                "access_token": self._access_token(),
                "vip_version": "v2",
            },
        )
        self._cached_profile = payload
        return payload

    def get_quota(self) -> dict[str, Any]:
        return self._request_json(
            "GET",
            f"{PAN_BASE_URL}/api/quota",
            params={
                "access_token": self._access_token(),
                "checkfree": 1,
                "checkexpire": 1,
            },
        )

    def list_files(
        self,
        directory: str | None = None,
        *,
        order: str = "name",
        desc: bool = False,
        start: int = 0,
        limit: int = 1000,
        folders_only: bool = False,
    ) -> dict[str, Any]:
        remote_dir = self.normalize_remote_path(directory or "/")
        payload = self._request_json(
            "GET",
            f"{PAN_BASE_URL}/rest/2.0/xpan/file",
            params={
                "method": "list",
                "access_token": self._access_token(),
                "dir": remote_dir,
                "order": order,
                "desc": 1 if desc else 0,
                "start": start,
                "limit": limit,
                "web": 1,
                "folder": 1 if folders_only else 0,
                "showempty": 1,
            },
        )
        payload["cwd"] = remote_dir
        return payload

    def get_entry_by_path(self, remote_path: str) -> dict[str, Any]:
        remote_path = self.normalize_remote_path(remote_path)
        parent = posixpath.dirname(remote_path) or "/"
        if parent == remote_path:
            parent = "/"
        listing = self.list_files(parent)
        for item in listing.get("list", []):
            if item.get("path") == remote_path:
                return item
        raise ApiError(f"Remote path not found: {remote_path}", code=-9)

    def get_file_metas(
        self,
        fsids: Iterable[int],
        *,
        include_dlink: bool = False,
        include_thumb: bool = False,
        include_extra: bool = False,
        include_media: bool = False,
        include_detail: bool = False,
    ) -> dict[str, Any]:
        fsid_list = [int(fsid) for fsid in fsids]
        if not fsid_list:
            raise ConfigurationError("At least one fs_id is required.")
        return self._request_json(
            "GET",
            f"{PAN_BASE_URL}/rest/2.0/xpan/multimedia",
            params={
                "method": "filemetas",
                "access_token": self._access_token(),
                "fsids": json.dumps(fsid_list, ensure_ascii=False, separators=(",", ":")),
                "dlink": 1 if include_dlink else 0,
                "thumb": 1 if include_thumb else 0,
                "extra": 1 if include_extra else 0,
                "needmedia": 1 if include_media else 0,
                "detail": 1 if include_detail else 0,
            },
        )

    def create_folder(self, remote_path: str, *, rename_on_conflict: bool = False) -> dict[str, Any]:
        now = int(time.time())
        return self._request_json(
            "POST",
            f"{PAN_BASE_URL}/rest/2.0/xpan/file",
            params={
                "method": "create",
                "access_token": self._access_token(),
            },
            data={
                "path": self.normalize_remote_path(remote_path),
                "isdir": 1,
                "rtype": 1 if rename_on_conflict else 0,
                "local_ctime": now,
                "local_mtime": now,
                "mode": 1,
            },
        )

    def rename(self, remote_path: str, new_name: str) -> dict[str, Any]:
        return self._file_manager(
            "rename",
            [{"path": self.normalize_remote_path(remote_path), "newname": new_name}],
        )

    def move(self, remote_path: str, destination_dir: str, *, new_name: str | None = None) -> dict[str, Any]:
        item: dict[str, Any] = {
            "path": self.normalize_remote_path(remote_path),
            "dest": self.normalize_remote_path(destination_dir),
        }
        if new_name:
            item["newname"] = new_name
        return self._file_manager("move", [item], ondup="overwrite")

    def delete(self, remote_paths: Iterable[str]) -> dict[str, Any]:
        paths = [self.normalize_remote_path(path) for path in remote_paths]
        return self._file_manager("delete", paths)

    def ensure_remote_directory(self, remote_dir: str) -> str:
        normalized = self.normalize_remote_path(remote_dir)
        if normalized == "/":
            return normalized

        parts = [part for part in normalized.split("/") if part]
        current = "/"
        for part in parts:
            current = self.normalize_remote_path(posixpath.join(current, part))
            try:
                entry = self.get_entry_by_path(current)
            except ApiError as exc:
                if exc.code != -9:
                    raise
                self.create_folder(current)
                continue
            if not bool(entry.get("isdir")):
                raise ConfigurationError(f"Remote path is not a directory: {current}")
        return normalized

    def resolve_upload_target(self, source_name: str, remote_path: str | None = None) -> str:
        raw_path = (remote_path or "").replace("\\", "/").strip()
        if not raw_path:
            return self.normalize_remote_path(source_name)

        if raw_path.endswith("/"):
            return self.normalize_remote_path(posixpath.join(raw_path, source_name))

        normalized = self.normalize_remote_path(raw_path)
        try:
            entry = self.get_entry_by_path(normalized)
        except ApiError as exc:
            if exc.code != -9:
                raise
            return normalized

        if bool(entry.get("isdir")):
            return self.normalize_remote_path(
                posixpath.join(str(entry.get("path") or normalized), source_name)
            )
        return normalized

    def upload_file(
        self,
        local_path: str | Path,
        remote_path: str | None = None,
        *,
        policy: str = "overwrite",
        prefer_single_step: bool = False,
        progress_callback: UploadProgressCallback | None = None,
    ) -> dict[str, Any]:
        source = Path(local_path)
        if not source.is_file():
            raise ConfigurationError(f"Local file does not exist: {source}")

        remote_target = self.resolve_upload_target(source.name, remote_path)
        total_size = int(source.stat().st_size)
        volume_limit = self.volume_split_bytes()
        if total_size <= volume_limit:
            return self._upload_single_file(
                source,
                remote_target,
                policy=policy,
                prefer_single_step=prefer_single_step,
                progress_callback=progress_callback,
            )

        volume_count = max(2, math.ceil(total_size / volume_limit))
        width = max(3, len(str(volume_count)))
        volume_root = self._build_volume_bundle_dir(remote_target)
        volume_dir = self.ensure_remote_directory(volume_root)
        volume_specs = [
            UploadVolumeSpec(
                index=index,
                start=index * volume_limit,
                size=min(volume_limit, total_size - (index * volume_limit)),
                target=self._build_volume_remote_path(
                    posixpath.join(volume_dir, Path(remote_target).name),
                    index=index,
                    total=volume_count,
                    width=width,
                ),
            )
            for index in range(volume_count)
        ]
        uploaded_volumes: list[dict[str, Any]] = []
        progress = _MultiVolumeUploadProgress(
            progress_callback,
            label=volume_dir,
            total_bytes=total_size,
            volume_count=volume_count,
        )
        remaining_specs: list[UploadVolumeSpec] = []
        for spec in volume_specs:
            existing_entry = self._find_resumable_volume_entry(spec.target, expected_size=spec.size)
            if existing_entry is not None:
                spec.existing_entry = existing_entry
                uploaded_volumes.append(
                    {
                        "index": spec.index + 1,
                        "path": str(existing_entry.get("path") or spec.target),
                        "size": spec.size,
                        "offset": spec.start,
                        "result": existing_entry,
                        "resumed": True,
                    }
                )
                progress_callback_for_spec = progress.callback_for(spec.index + 1)
                if progress_callback_for_spec is not None:
                    progress_callback_for_spec(
                        {
                            "phase": "completed",
                            "label": str(existing_entry.get("path") or spec.target),
                            "transferred_bytes": spec.size,
                            "total_bytes": spec.size,
                            "delta_bytes": 0,
                            "volume_index": spec.index + 1,
                            "volume_count": volume_count,
                        }
                    )
                logger.info(
                    "Resume multi-volume upload by reusing remote volume %s (%s bytes)",
                    str(existing_entry.get("path") or spec.target),
                    spec.size,
                )
                continue
            remaining_specs.append(spec)

        if not remaining_specs:
            script_result = self.upload_text_file(
                self._build_extract_script(Path(remote_target).name, volume_specs),
                posixpath.join(volume_dir, "extract.sh"),
                ondup="overwrite",
            )
            uploaded_volumes.sort(key=lambda item: int(item.get("index", 0) or 0))
            return {
                "path": volume_dir,
                "is_multi_volume": True,
                "bundle_dir": volume_dir,
                "extract_script": script_result.get("path") or posixpath.join(volume_dir, "extract.sh"),
                "volume_size_limit": volume_limit,
                "volume_count": volume_count,
                "volumes": uploaded_volumes,
            }

        first_spec = self._prepare_upload_volume(
            source,
            remaining_specs[0],
            prefer_single_step=prefer_single_step,
            progress_callback=progress_callback,
            volume_count=volume_count,
            report_as_prepare=False,
        )
        volume_workers = min(self.config.effective_upload_volume_workers(), volume_count)
        next_spec_index = 1
        next_prepare_future = None
        pending_uploads: dict[Any, UploadVolumeSpec] = {}
        prepared_specs: deque[UploadVolumeSpec] = deque()

        def _submit_upload(prepared_spec: UploadVolumeSpec, upload_executor: ThreadPoolExecutor) -> None:
            future = upload_executor.submit(
                self._upload_single_file,
                source,
                prepared_spec.target,
                policy="overwrite",
                prefer_single_step=prefer_single_step,
                progress_callback=progress.callback_for(prepared_spec.index + 1),
                byte_range=(prepared_spec.start, prepared_spec.size),
                digest_plan=prepared_spec.digest_plan,
                volume_index=prepared_spec.index + 1,
                volume_count=volume_count,
            )
            pending_uploads[future] = prepared_spec

        with ThreadPoolExecutor(max_workers=1) as prepare_executor, ThreadPoolExecutor(
            max_workers=volume_workers
        ) as upload_executor:
            _submit_upload(first_spec, upload_executor)
            if next_spec_index < len(remaining_specs):
                next_prepare_future = prepare_executor.submit(
                    self._prepare_upload_volume,
                    source,
                    remaining_specs[next_spec_index],
                    prefer_single_step=prefer_single_step,
                    progress_callback=progress_callback,
                    volume_count=volume_count,
                    report_as_prepare=True,
                )
                next_spec_index += 1

            while pending_uploads or next_prepare_future is not None or prepared_specs:
                while next_prepare_future is not None and next_prepare_future.done():
                    prepared_specs.append(next_prepare_future.result())
                    if next_spec_index < len(remaining_specs):
                        next_prepare_future = prepare_executor.submit(
                            self._prepare_upload_volume,
                            source,
                            remaining_specs[next_spec_index],
                            prefer_single_step=prefer_single_step,
                            progress_callback=progress_callback,
                            volume_count=volume_count,
                            report_as_prepare=True,
                        )
                        next_spec_index += 1
                    else:
                        next_prepare_future = None

                while prepared_specs and len(pending_uploads) < volume_workers:
                    _submit_upload(prepared_specs.popleft(), upload_executor)

                if pending_uploads:
                    done, _ = wait(list(pending_uploads.keys()), timeout=0.2, return_when=FIRST_COMPLETED)
                    for future in done:
                        prepared_spec = pending_uploads.pop(future)
                        result = future.result()
                        uploaded_volumes.append(
                            {
                                "index": prepared_spec.index + 1,
                                "path": result.get("path") or prepared_spec.target,
                                "size": prepared_spec.size,
                                "offset": prepared_spec.start,
                                "result": result,
                            }
                        )
                elif next_prepare_future is not None:
                    prepared_specs.append(next_prepare_future.result())
                    if next_spec_index < len(remaining_specs):
                        next_prepare_future = prepare_executor.submit(
                            self._prepare_upload_volume,
                            source,
                            remaining_specs[next_spec_index],
                            prefer_single_step=prefer_single_step,
                            progress_callback=progress_callback,
                            volume_count=volume_count,
                            report_as_prepare=True,
                        )
                        next_spec_index += 1
                    else:
                        next_prepare_future = None

        script_result = self.upload_text_file(
            self._build_extract_script(Path(remote_target).name, volume_specs),
            posixpath.join(volume_dir, "extract.sh"),
            ondup="overwrite",
        )
        uploaded_volumes.sort(key=lambda item: int(item.get("index", 0) or 0))
        return {
            "path": volume_dir,
            "is_multi_volume": True,
            "bundle_dir": volume_dir,
            "extract_script": script_result.get("path") or posixpath.join(volume_dir, "extract.sh"),
            "volume_size_limit": volume_limit,
            "volume_count": volume_count,
            "volumes": uploaded_volumes,
        }

    def _find_resumable_volume_entry(
        self,
        remote_path: str,
        *,
        expected_size: int,
    ) -> dict[str, Any] | None:
        try:
            entry = self.get_entry_by_path(remote_path)
        except ApiError as exc:
            if exc.code == -9:
                return None
            raise
        if bool(entry.get("isdir")):
            return None
        remote_size = int(entry.get("size", 0) or 0)
        if remote_size != int(expected_size):
            return None
        return entry

    def _upload_single_file(
        self,
        source: Path,
        remote_target: str,
        *,
        policy: str,
        prefer_single_step: bool,
        progress_callback: UploadProgressCallback | None = None,
        byte_range: tuple[int, int] | None = None,
        digest_plan: UploadDigestPlan | None = None,
        volume_index: int = 0,
        volume_count: int = 0,
    ) -> dict[str, Any]:
        range_start, range_size = self._normalize_byte_range(source, byte_range)
        uploaded_bytes = 0
        chunk_size = self.upload_chunk_size()
        with requests.Session() as upload_session:
            if prefer_single_step and range_size <= min(chunk_size, 2_000_000_000):
                result = self.upload_file_single_step(
                    source,
                    remote_target,
                    ondup=policy,
                    progress_callback=progress_callback,
                    byte_range=(range_start, range_size),
                    session=upload_session,
                )
                self._report_upload_progress(
                    progress_callback,
                    phase="completed",
                    label=remote_target,
                    transferred_bytes=range_size,
                    total_bytes=range_size,
                    volume_index=volume_index,
                    volume_count=volume_count,
                )
                return result

            if digest_plan is None:
                digest_plan = self._build_upload_digests(
                    source,
                    progress_callback=progress_callback,
                    label=remote_target,
                    byte_range=(range_start, range_size),
                )
            precreate = self._request_json(
                "POST",
                f"{PAN_BASE_URL}/rest/2.0/xpan/file",
                params={
                    "method": "precreate",
                    "access_token": self._access_token(),
                },
                data={
                    "path": remote_target,
                    "size": digest_plan.size,
                    "isdir": 0,
                    "autoinit": 1,
                    "rtype": self._rtype_from_policy(policy),
                    "block_list": json.dumps(
                        digest_plan.block_list, ensure_ascii=False, separators=(",", ":")
                    ),
                    "content-md5": digest_plan.content_md5,
                    "slice-md5": digest_plan.slice_md5,
                    "local_ctime": digest_plan.local_ctime,
                    "local_mtime": digest_plan.local_mtime,
                },
                session=upload_session,
            )

            upload_id = str(precreate.get("uploadid", "")).strip()
            if not upload_id:
                raise ApiError("precreate did not return uploadid", payload=precreate)

            upload_server = self.locate_upload_server(remote_target, upload_id, session=upload_session)
            missing_parts = self._normalize_missing_parts(precreate.get("block_list"))
            if not missing_parts:
                uploaded_bytes = range_size
            self._report_upload_progress(
                progress_callback,
                phase="uploading",
                label=remote_target,
                transferred_bytes=uploaded_bytes,
                total_bytes=range_size,
                volume_index=volume_index,
                volume_count=volume_count,
            )
            for index in missing_parts:
                self._upload_part(
                    server_url=upload_server,
                    remote_path=remote_target,
                    upload_id=upload_id,
                    part_index=index,
                    file_path=source,
                    progress_callback=progress_callback,
                    total_bytes=range_size,
                    transferred_bytes=uploaded_bytes,
                    label=remote_target,
                    byte_range=(range_start, range_size),
                    session=upload_session,
                )
                part_size = min(chunk_size, max(0, range_size - (index * chunk_size)))
                uploaded_bytes += part_size

            result = self._request_json(
                "POST",
                f"{PAN_BASE_URL}/rest/2.0/xpan/file",
                params={
                    "method": "create",
                    "access_token": self._access_token(),
                },
                data={
                    "path": remote_target,
                    "size": digest_plan.size,
                    "isdir": 0,
                    "rtype": self._rtype_from_policy(policy),
                    "uploadid": upload_id,
                    "block_list": json.dumps(
                        digest_plan.block_list, ensure_ascii=False, separators=(",", ":")
                    ),
                    "local_ctime": digest_plan.local_ctime,
                    "local_mtime": digest_plan.local_mtime,
                },
                session=upload_session,
            )
            self._report_upload_progress(
                progress_callback,
                phase="completed",
                label=remote_target,
                transferred_bytes=range_size,
                total_bytes=range_size,
                volume_index=volume_index,
                volume_count=volume_count,
            )
            return result

    def _prepare_upload_volume(
        self,
        source: Path,
        spec: UploadVolumeSpec,
        *,
        prefer_single_step: bool,
        progress_callback: UploadProgressCallback | None = None,
        volume_count: int = 0,
        report_as_prepare: bool = False,
    ) -> UploadVolumeSpec:
        chunk_size = self.upload_chunk_size()
        if prefer_single_step and spec.size <= min(chunk_size, 2_000_000_000):
            return spec
        callback = progress_callback
        if progress_callback and report_as_prepare:
            callback = lambda event: progress_callback(
                {
                    **event,
                    "phase": "preparing",
                    "stream": "prepare",
                    "volume_index": int(spec.index + 1),
                    "volume_count": int(volume_count),
                }
            )
        elif progress_callback:
            callback = lambda event: progress_callback(
                {
                    **event,
                    "volume_index": int(spec.index + 1),
                    "volume_count": int(volume_count),
                }
            )
        spec.digest_plan = self._build_upload_digests(
            source,
            progress_callback=callback,
            label=spec.target,
            byte_range=(spec.start, spec.size),
        )
        return spec

    def upload_file_single_step(
        self,
        local_path: str | Path,
        remote_path: str,
        *,
        ondup: str = "overwrite",
        progress_callback: UploadProgressCallback | None = None,
        byte_range: tuple[int, int] | None = None,
        session: requests.Session | None = None,
    ) -> dict[str, Any]:
        source = Path(local_path)
        remote_target = self.normalize_remote_path(remote_path)
        upload_server = "https://c3.pcs.baidu.com"
        range_start, total_size = self._normalize_byte_range(source, byte_range)
        self._report_upload_progress(
            progress_callback,
            phase="uploading",
            label=remote_target,
            transferred_bytes=0,
            total_bytes=total_size,
        )
        with source.open("rb") as handle:
            state = {"sent": 0}
            reader = _RangeReader(handle, start=range_start, length=total_size)
            wrapped = _ProgressReader(
                reader,
                callback=lambda size: (
                    state.__setitem__("sent", state["sent"] + size),
                    self._report_upload_progress(
                        progress_callback,
                        phase="uploading",
                        label=remote_target,
                        transferred_bytes=state["sent"],
                        total_bytes=total_size,
                        delta_bytes=size,
                    ),
                )[-1],
            )
            return self._request_json(
                "POST",
                f"{upload_server}/rest/2.0/pcs/file",
                params={
                    "method": "upload",
                    "access_token": self._access_token(),
                    "path": remote_target,
                    "ondup": self._ondup_from_policy(ondup),
                },
                files={"file": (source.name, wrapped)},
                session=session,
                timeout=DEFAULT_UPLOAD_TIMEOUT,
                retries=DEFAULT_UPLOAD_RETRIES,
                retry_backoff=DEFAULT_UPLOAD_RETRY_BACKOFF,
            )

    def upload_text_file(
        self,
        content: str,
        remote_path: str,
        *,
        ondup: str = "overwrite",
    ) -> dict[str, Any]:
        remote_target = self.normalize_remote_path(remote_path)
        upload_server = "https://c3.pcs.baidu.com"
        payload = content.encode("utf-8")
        return self._request_json(
            "POST",
            f"{upload_server}/rest/2.0/pcs/file",
            params={
                "method": "upload",
                "access_token": self._access_token(),
                "path": remote_target,
                "ondup": self._ondup_from_policy(ondup),
            },
            files={"file": (Path(remote_target).name, io.BytesIO(payload))},
        )

    def locate_upload_server(
        self,
        remote_path: str,
        upload_id: str,
        *,
        session: requests.Session | None = None,
    ) -> str:
        remote_target = self.normalize_remote_path(remote_path)
        payload = self._request_json(
            "GET",
            f"{PCS_BASE_URL}/rest/2.0/pcs/file",
            params={
                "method": "locateupload",
                "appid": NETDISK_APP_ID,
                "access_token": self._access_token(),
                "path": remote_target,
                "uploadid": upload_id,
                "upload_version": "2.0",
            },
            session=session,
        )

        servers = payload.get("servers") or []
        if servers:
            candidate = servers[0].get("server", "")
            if candidate:
                return candidate.rstrip("/")

        host = str(payload.get("host", "")).strip()
        if host:
            return f"https://{host}".rstrip("/")

        return PCS_BASE_URL

    def build_download_spec(self, fs_id: int) -> DownloadSpec:
        payload = self.get_file_metas([fs_id], include_dlink=True)
        items = payload.get("list", [])
        if not items:
            raise ApiError("filemetas returned an empty list", payload=payload)
        item = items[0]
        dlink = str(item.get("dlink", "")).strip()
        if not dlink:
            raise ApiError("No dlink was returned for the requested file.", payload=item)
        if "access_token=" not in dlink:
            separator = "&" if "?" in dlink else "?"
            dlink = f"{dlink}{separator}access_token={self._access_token()}"
        remote_md5 = str(item.get("md5", "") or "").strip().lower()
        if remote_md5 and not MD5_RE.fullmatch(remote_md5):
            logger.warning(
                "Ignore invalid remote md5 for %s: %s",
                str(item.get("path", "")).strip() or fs_id,
                remote_md5,
            )
            remote_md5 = ""
        return DownloadSpec(
            file_name=str(item.get("filename", "download.bin")).strip() or "download.bin",
            file_path=str(item.get("path", "")).strip(),
            dlink=dlink,
            size=int(item.get("size", 0) or 0),
            md5=remote_md5,
        )

    def open_download(
        self,
        fs_id: int,
        *,
        byte_range: str | None = None,
        spec: DownloadSpec | None = None,
        session: requests.Session | None = None,
    ) -> tuple[DownloadSpec, requests.Response]:
        spec = spec or self.build_download_spec(fs_id)
        client = session or self.session
        headers = self._download_headers()
        if byte_range:
            headers["Range"] = byte_range
        response = client.get(
            spec.dlink,
            headers=headers,
            stream=True,
            timeout=120,
            allow_redirects=False,
        )
        if response.is_redirect:
            location = response.headers.get("Location")
            response.close()
            if not location:
                raise ApiError("Download redirect did not include a Location header.")
            final_headers = self._base_headers()
            if byte_range:
                final_headers["Range"] = byte_range
            response = client.get(
                location,
                headers=final_headers,
                stream=True,
                timeout=120,
                allow_redirects=True,
            )
        if response.status_code >= 400:
            body = response.text
            raise ApiError(
                f"Download failed with HTTP {response.status_code}: {body[:200]}",
                code=response.status_code,
            )
        return spec, response

    def _report_download_progress(
        self,
        callback: DownloadProgressCallback | None,
        *,
        phase: str,
        label: str,
        downloaded_bytes: int = 0,
        download_total_bytes: int = 0,
        download_delta_bytes: int = 0,
        verify_bytes: int = 0,
        verify_total_bytes: int = 0,
    ) -> None:
        if callback:
            callback(
                {
                    "phase": phase,
                    "label": label,
                    "downloaded_bytes": int(downloaded_bytes),
                    "download_total_bytes": int(download_total_bytes),
                    "download_delta_bytes": int(download_delta_bytes),
                    "verify_bytes": int(verify_bytes),
                    "verify_total_bytes": int(verify_total_bytes),
                }
            )

    def _report_upload_progress(
        self,
        callback: UploadProgressCallback | None,
        *,
        phase: str,
        label: str,
        transferred_bytes: int = 0,
        total_bytes: int = 0,
        delta_bytes: int = 0,
        incremental: bool = False,
        volume_index: int = 0,
        volume_count: int = 0,
    ) -> None:
        if callback:
            callback(
                {
                    "phase": phase,
                    "label": label,
                    "transferred_bytes": int(transferred_bytes),
                    "total_bytes": int(total_bytes),
                    "delta_bytes": int(delta_bytes),
                    "incremental": bool(incremental),
                    "volume_index": int(volume_index),
                    "volume_count": int(volume_count),
                }
            )

    def _calculate_file_md5(
        self,
        file_path: Path,
        *,
        label: str,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> str:
        digest = hashlib.md5()
        total_bytes = int(file_path.stat().st_size)
        done_bytes = 0
        self._report_download_progress(
            progress_callback,
            phase="verifying",
            label=label,
            verify_bytes=0,
            verify_total_bytes=total_bytes,
        )
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(CHECKSUM_READ_SIZE), b""):
                if chunk:
                    digest.update(chunk)
                    done_bytes += len(chunk)
                    self._report_download_progress(
                        progress_callback,
                        phase="verifying",
                        label=label,
                        verify_bytes=done_bytes,
                        verify_total_bytes=total_bytes,
                    )
        return digest.hexdigest()

    def _ensure_download_md5(
        self,
        file_path: Path,
        *,
        spec: DownloadSpec,
        label: str,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> None:
        if not spec.md5:
            logger.info("Skip MD5 verify for %s because remote metadata has no md5", label)
            return
        logger.info("Start MD5 verify for %s (%s)", label, file_path)
        actual_md5 = self._calculate_file_md5(
            file_path,
            label=label,
            progress_callback=progress_callback,
        )
        if actual_md5.lower() != spec.md5.lower():
            logger.warning(
                "MD5 verify failed for %s: expected=%s actual=%s",
                label,
                spec.md5,
                actual_md5,
            )
            raise ApiError(
                f"MD5 mismatch for {label}: expected {spec.md5}, got {actual_md5}",
            )
        logger.info("MD5 verify passed for %s", label)

    def _should_parallel_download_single_file(
        self,
        *,
        expected_size: int,
        offset: int,
        single_file_parallel: bool | None,
    ) -> bool:
        if single_file_parallel is False:
            return False
        if offset:
            return False
        if expected_size < MIN_SINGLE_FILE_PARALLEL_SIZE:
            return False
        if self.single_file_download_worker_count() <= 1:
            return False
        enabled = bool(getattr(self.config, "single_file_parallel_enabled", True))
        return enabled if single_file_parallel is None else bool(single_file_parallel)

    def _parallel_download_part_ranges(
        self,
        expected_size: int,
        requested_workers: int,
    ) -> list[tuple[int, int]]:
        if expected_size <= 0:
            return []
        max_useful_workers = max(1, math.ceil(expected_size / MIN_SINGLE_FILE_PARALLEL_SIZE))
        worker_count = max(1, min(requested_workers, max_useful_workers))
        if worker_count <= 1:
            return []
        base_size = expected_size // worker_count
        remainder = expected_size % worker_count
        ranges: list[tuple[int, int]] = []
        start = 0
        for index in range(worker_count):
            part_size = base_size + (1 if index < remainder else 0)
            end = start + part_size - 1
            ranges.append((start, end))
            start = end + 1
        return [item for item in ranges if item[0] <= item[1]]

    def _supports_parallel_download(self, fs_id: int, spec: DownloadSpec) -> bool:
        try:
            _, response = self.open_download(
                fs_id,
                byte_range="bytes=0-0",
                spec=spec,
            )
        except ApiError:
            return False
        try:
            return response.status_code == 206 and "Content-Range" in response.headers
        finally:
            response.close()

    def _download_file_in_parallel(
        self,
        fs_id: int,
        *,
        spec: DownloadSpec,
        expected_size: int,
        partial_target: Path,
        label: str,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> bool:
        ranges = self._parallel_download_part_ranges(
            expected_size,
            self.single_file_download_worker_count(),
        )
        if len(ranges) <= 1:
            return False
        if not self._supports_parallel_download(fs_id, spec):
            logger.info("Parallel download is not supported for %s, falling back to single stream", label)
            return False

        part_progress = [0 for _ in ranges]
        part_targets = [partial_target.with_name(f"{partial_target.name}.part{index}") for index in range(len(ranges))]
        progress_lock = threading.Lock()

        for part_target in part_targets:
            part_target.unlink(missing_ok=True)
        partial_target.unlink(missing_ok=True)

        def report(index: int, delta: int) -> None:
            with progress_lock:
                part_progress[index] += delta
                downloaded_bytes = sum(part_progress)
            self._report_download_progress(
                progress_callback,
                phase="downloading",
                label=label,
                downloaded_bytes=downloaded_bytes,
                download_total_bytes=expected_size,
                download_delta_bytes=delta,
            )

        def worker(index: int, start: int, end: int) -> None:
            byte_range = f"bytes={start}-{end}"
            session = requests.Session()
            try:
                _, response = self.open_download(
                    fs_id,
                    byte_range=byte_range,
                    spec=spec,
                    session=session,
                )
                try:
                    if response.status_code != 206:
                        raise ApiError(
                            f"Parallel download was rejected for {spec.file_name}: HTTP {response.status_code}",
                            code=response.status_code,
                        )
                    with part_targets[index].open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if not chunk:
                                continue
                            handle.write(chunk)
                            report(index, len(chunk))
                finally:
                    response.close()
            except Exception:
                raise
            finally:
                session.close()

        try:
            with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
                futures = [
                    executor.submit(worker, index, start, end)
                    for index, (start, end) in enumerate(ranges)
                ]
                for future in futures:
                    future.result()

            with partial_target.open("wb") as merged:
                for index, (start, end) in enumerate(ranges):
                    expected_part_size = end - start + 1
                    actual_part_size = part_targets[index].stat().st_size if part_targets[index].exists() else 0
                    if actual_part_size != expected_part_size:
                        raise ApiError(
                            f"Parallel download incomplete for {spec.file_name} part {index}: expected {expected_part_size} bytes, got {actual_part_size}",
                        )
                    with part_targets[index].open("rb") as part_handle:
                        for chunk in iter(lambda: part_handle.read(1024 * 1024), b""):
                            if chunk:
                                merged.write(chunk)
        except Exception:
            partial_target.unlink(missing_ok=True)
            for part_target in part_targets:
                part_target.unlink(missing_ok=True)
            raise
        else:
            for part_target in part_targets:
                part_target.unlink(missing_ok=True)
            return True

    def download_file(
        self,
        remote_path: str,
        destination: str | Path | None = None,
        *,
        resume: bool = True,
        progress_callback: DownloadProgressCallback | None = None,
        parallel: bool = True,
        single_file_parallel: bool | None = None,
    ) -> Path:
        entry = self.get_entry_by_path(remote_path)
        if bool(entry.get("isdir")):
            return self.download_directory(
                remote_path,
                destination,
                resume=resume,
                progress_callback=progress_callback,
                parallel=parallel,
                single_file_parallel=single_file_parallel,
            )
        return self._download_entry_to_path(
            entry,
            destination,
            resume=resume,
            progress_callback=progress_callback,
            single_file_parallel=single_file_parallel,
        )

    def download_directory(
        self,
        remote_path: str,
        destination: str | Path | None = None,
        *,
        resume: bool = True,
        progress_callback: DownloadProgressCallback | None = None,
        parallel: bool = True,
        single_file_parallel: bool | None = None,
    ) -> Path:
        entry = self.get_entry_by_path(remote_path)
        if not bool(entry.get("isdir")):
            return self._download_entry_to_path(
                entry,
                destination,
                resume=resume,
                progress_callback=progress_callback,
            )

        default_name = Path(str(entry.get("path", remote_path))).name or "download"
        target_root = Path(destination or default_name)
        if target_root.exists() and target_root.is_file():
            raise ConfigurationError(f"Download destination is a file: {target_root}")
        target_root.mkdir(parents=True, exist_ok=True)

        children = self._iter_directory_entries(str(entry.get("path") or remote_path))
        for child in children:
            if bool(child.get("isdir")):
                relative = child["relative_path"]
                target_root.joinpath(*relative.split("/")).mkdir(parents=True, exist_ok=True)

        file_jobs = []
        for child in children:
            if bool(child.get("isdir")):
                continue
            relative = child["relative_path"]
            file_jobs.append((child, target_root.joinpath(*relative.split("/"))))

        if not file_jobs:
            return target_root

        if not parallel:
            for child, local_target in file_jobs:
                self._download_entry_to_path(
                    child,
                    local_target,
                    resume=resume,
                    progress_callback=progress_callback,
                    single_file_parallel=single_file_parallel,
                )
            return target_root

        worker_count = min(self.download_worker_count(), max(1, len(file_jobs)))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(
                    self._download_entry_to_path,
                    child,
                    local_target,
                    resume=resume,
                    progress_callback=progress_callback,
                    single_file_parallel=single_file_parallel,
                )
                for child, local_target in file_jobs
            ]
            for future in futures:
                future.result()
        return target_root

    def _download_entry_to_path(
        self,
        entry: dict[str, Any],
        destination: str | Path | None = None,
        *,
        resume: bool = True,
        progress_callback: DownloadProgressCallback | None = None,
        single_file_parallel: bool | None = None,
    ) -> Path:
        fs_id = int(entry["fs_id"])
        file_name = str(entry.get("server_filename") or Path(str(entry.get("path", ""))).name or "download.bin")
        label = str(entry.get("path") or file_name)
        target = Path(destination or file_name)
        expected_size = int(entry.get("size", 0) or 0)
        spec = self.build_download_spec(fs_id)
        if target.exists() and target.is_dir():
            target = target / file_name
        target.parent.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Prepare download for %s -> %s (size=%s, resume=%s)",
            label,
            target,
            expected_size,
            resume,
        )

        partial_target = target.with_name(f"{target.name}.part")
        if target.exists() and target.is_file():
            current_size = target.stat().st_size
            if expected_size and current_size == expected_size:
                logger.info("Found existing completed file for %s, verifying before reuse", label)
                try:
                    self._ensure_download_md5(
                        target,
                        spec=spec,
                        label=label,
                        progress_callback=progress_callback,
                    )
                except ApiError:
                    logger.warning("Existing local file failed verify for %s, deleting and downloading again", label)
                    target.unlink()
                    partial_target.unlink(missing_ok=True)
                else:
                    partial_target.unlink(missing_ok=True)
                    self._report_download_progress(
                        progress_callback,
                        phase="completed",
                        label=label,
                        downloaded_bytes=current_size,
                        download_total_bytes=expected_size,
                    )
                    logger.info("Reuse existing local file for %s", label)
                    return target
            if expected_size and current_size > expected_size:
                raise ConfigurationError(
                    f"Local file is larger than the remote file: {target}"
                )
            if resume and (not expected_size or current_size < expected_size) and not partial_target.exists():
                logger.info("Move incomplete local file into partial slot for %s", label)
                target.replace(partial_target)
            elif not resume:
                partial_target.unlink(missing_ok=True)

        offset = 0
        if resume and partial_target.exists():
            offset = partial_target.stat().st_size
            if expected_size and offset > expected_size:
                logger.warning("Discard stale partial file for %s because it is larger than remote", label)
                partial_target.unlink()
                offset = 0
            if expected_size and offset == expected_size:
                logger.info("Found completed partial file for %s, verifying before finalize", label)
                try:
                    self._ensure_download_md5(
                        partial_target,
                        spec=spec,
                        label=label,
                        progress_callback=progress_callback,
                    )
                except ApiError:
                    logger.warning("Completed partial file failed verify for %s, deleting and downloading again", label)
                    partial_target.unlink()
                    offset = 0
                else:
                    partial_target.replace(target)
                    self._report_download_progress(
                        progress_callback,
                        phase="completed",
                        label=label,
                        downloaded_bytes=offset,
                        download_total_bytes=expected_size,
                    )
                    logger.info("Finalize resumed file for %s", label)
                    return target

        self._report_download_progress(
            progress_callback,
            phase="downloading",
            label=label,
            downloaded_bytes=offset,
            download_total_bytes=expected_size,
        )
        if self._should_parallel_download_single_file(
            expected_size=expected_size,
            offset=offset,
            single_file_parallel=single_file_parallel,
        ):
            logger.info("Start parallel download for %s with workers=%s", label, self.single_file_download_worker_count())
            if self._download_file_in_parallel(
                fs_id,
                spec=spec,
                expected_size=expected_size,
                partial_target=partial_target,
                label=label,
                progress_callback=progress_callback,
            ):
                self._ensure_download_md5(
                    partial_target,
                    spec=spec,
                    label=label,
                    progress_callback=progress_callback,
                )
                partial_target.replace(target)
                self._report_download_progress(
                    progress_callback,
                    phase="completed",
                    label=label,
                    downloaded_bytes=expected_size,
                    download_total_bytes=expected_size,
                    verify_bytes=expected_size,
                    verify_total_bytes=expected_size,
                )
                logger.info("Parallel download completed for %s -> %s", label, target)
                return target

        byte_range = f"bytes={offset}-" if offset else None
        logger.info("Start network download for %s with offset=%s", label, offset)
        spec, response = self.open_download(fs_id, byte_range=byte_range, spec=spec)
        try:
            if offset and response.status_code != 206:
                raise ApiError(
                    f"Resume download was rejected for {spec.file_name}: HTTP {response.status_code}",
                    code=response.status_code,
                )
            download_target = partial_target if resume else target
            mode = "ab" if offset else "wb"
            with download_target.open(mode) as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
                        offset += len(chunk)
                        self._report_download_progress(
                            progress_callback,
                            phase="downloading",
                            label=label,
                            downloaded_bytes=offset,
                            download_total_bytes=expected_size,
                            download_delta_bytes=len(chunk),
                        )
        finally:
            response.close()

        completed_target = partial_target if resume else target
        if expected_size and completed_target.stat().st_size != expected_size:
            raise ApiError(
                f"Download incomplete for {completed_target.name}: expected {expected_size} bytes, got {completed_target.stat().st_size}",
            )
        self._ensure_download_md5(
            completed_target,
            spec=spec,
            label=label,
            progress_callback=progress_callback,
        )
        if resume:
            completed_target.replace(target)
        self._report_download_progress(
            progress_callback,
            phase="completed",
            label=label,
            downloaded_bytes=expected_size,
            download_total_bytes=expected_size,
            verify_bytes=expected_size,
            verify_total_bytes=expected_size,
        )
        logger.info("Download completed for %s -> %s", label, target)
        return target

    def _iter_directory_entries(self, remote_path: str, relative_prefix: str = "") -> list[dict[str, Any]]:
        listing = self.list_files(remote_path)
        items: list[dict[str, Any]] = []
        for item in listing.get("list", []):
            name = str(item.get("server_filename") or Path(str(item.get("path", ""))).name)
            relative_path = f"{relative_prefix}/{name}" if relative_prefix else name
            enriched = dict(item)
            enriched["relative_path"] = relative_path
            items.append(enriched)
            if bool(item.get("isdir")):
                items.extend(self._iter_directory_entries(str(item.get("path") or ""), relative_path))
        return items

    def download_paths(
        self,
        remote_paths: Iterable[str],
        destination_root: str | Path,
        *,
        resume: bool = True,
        progress_callback: Callable[[int, int, str], None] | None = None,
        parallel: bool = True,
        single_file_parallel: bool | None = None,
    ) -> list[Path]:
        base = Path(destination_root)
        if base.exists() and base.is_file():
            raise ConfigurationError(f"Download destination is a file: {base}")
        base.mkdir(parents=True, exist_ok=True)

        results: list[Path] = []
        for remote_path in remote_paths:
            entry = self.get_entry_by_path(remote_path)
            name = Path(str(entry.get("path", remote_path))).name or "download"
            local_target = base / name
            results.append(
                self.download_file(
                    str(entry.get("path") or remote_path),
                    local_target,
                    resume=resume,
                    progress_callback=progress_callback,
                    parallel=parallel,
                    single_file_parallel=single_file_parallel,
                )
            )
        return results

    def normalize_remote_path(self, raw_path: str | None) -> str:
        config = self.config
        root = config.resolved_app_root()
        if config.enforce_app_root and not root:
            raise ConfigurationError(
                "Please configure app_name or app_root before calling the API."
            )

        candidate = (raw_path or "/").replace("\\", "/").strip()
        if not candidate or candidate in {".", "/"}:
            normalized = root or "/"
        elif candidate.startswith("/apps/"):
            normalized = posixpath.normpath(candidate)
        else:
            if candidate.startswith("/"):
                candidate = candidate[1:]
            base = root or "/"
            normalized = posixpath.normpath(posixpath.join(base, candidate))

        if not normalized.startswith("/"):
            normalized = f"/{normalized}"

        if config.enforce_app_root and root:
            root_prefix = root.rstrip("/")
            if normalized != root and not normalized.startswith(f"{root_prefix}/"):
                raise ConfigurationError(
                    f"Path escapes the app root: {normalized} not under {root}"
                )

        return normalized

    def display_path(self, remote_path: str) -> str:
        root = self.config.resolved_app_root()
        if not root:
            return remote_path
        if remote_path == root:
            return "/"
        root_prefix = root.rstrip("/")
        if remote_path.startswith(f"{root_prefix}/"):
            return "/" + remote_path[len(root_prefix) + 1 :]
        return remote_path

    def _access_token(self) -> str:
        token = self.token
        if not token or not token.access_token:
            raise AuthenticationError("No access token found. Please authorize first.")
        if token.is_expired():
            token = self.refresh_access_token()
        return token.access_token

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        auth_request: bool = False,
        session: requests.Session | None = None,
        timeout: float | tuple[float, float] = DEFAULT_API_TIMEOUT,
        retries: int = 0,
        retry_backoff: float = 0.0,
    ) -> dict[str, Any]:
        headers = self._base_headers()
        if auth_request:
            headers.pop("Host", None)
        client = session or self.session
        attempt = 0
        while True:
            try:
                response = client.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    files=files,
                    headers=headers,
                    timeout=timeout,
                )
                break
            except requests.Timeout as exc:
                if attempt >= retries:
                    raise ApiError(f"Baidu API request timed out: {exc}") from exc
                attempt += 1
                delay = retry_backoff * attempt if retry_backoff > 0 else 0.0
                logger.warning(
                    "Retry upload request after timeout (%s/%s): %s %s",
                    attempt,
                    retries,
                    method,
                    url,
                )
                if delay > 0:
                    time.sleep(delay)
            except requests.ConnectionError as exc:
                if attempt >= retries:
                    raise ApiError(f"Baidu API connection failed: {exc}") from exc
                attempt += 1
                delay = retry_backoff * attempt if retry_backoff > 0 else 0.0
                logger.warning(
                    "Retry upload request after connection error (%s/%s): %s %s",
                    attempt,
                    retries,
                    method,
                    url,
                )
                if delay > 0:
                    time.sleep(delay)
            except requests.RequestException as exc:
                raise ApiError(f"Baidu API request failed: {exc}") from exc
        if response.status_code >= 400:
            snippet = response.text[:500]
            raise ApiError(
                f"HTTP {response.status_code} returned by Baidu API: {snippet}",
                code=response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiError(
                "Expected JSON response from Baidu API.",
                payload={"body": response.text[:500]},
            ) from exc
        self._raise_api_payload_error(payload)
        return payload

    def _raise_api_payload_error(self, payload: dict[str, Any]) -> None:
        if "error" in payload:
            description = payload.get("error_description") or payload.get("error")
            raise AuthenticationError(str(description))

        for key in ("errno", "error_code"):
            if key in payload and payload[key] not in (0, "0", None, ""):
                message = payload.get("errmsg") or payload.get("error_msg") or "Baidu API error"
                raise ApiError(str(message), code=payload[key], payload=payload)

    def _file_manager(
        self,
        opera: str,
        filelist: list[Any],
        *,
        ondup: str | None = None,
        async_mode: int = 0,
    ) -> dict[str, Any]:
        payload = {
            "async": async_mode,
            "filelist": json.dumps(filelist, ensure_ascii=False, separators=(",", ":")),
        }
        if ondup:
            payload["ondup"] = ondup
        return self._request_json(
            "POST",
            f"{PAN_BASE_URL}/rest/2.0/xpan/file",
            params={
                "method": "filemanager",
                "access_token": self._access_token(),
                "opera": opera,
            },
            data=payload,
        )

    def _build_upload_digests(
        self,
        file_path: Path,
        *,
        progress_callback: UploadProgressCallback | None = None,
        label: str | None = None,
        byte_range: tuple[int, int] | None = None,
    ) -> UploadDigestPlan:
        stat = file_path.stat()
        range_start, file_size = self._normalize_byte_range(file_path, byte_range)
        content_md5 = hashlib.md5()
        block_list: list[str] = []
        self._report_upload_progress(
            progress_callback,
            phase="hashing",
            label=label or str(file_path),
            transferred_bytes=0,
            total_bytes=file_size,
        )

        with file_path.open("rb") as handle:
            handle.seek(range_start)
            first_slice = handle.read(min(FIRST_SLICE_SIZE, file_size))
        first_slice_md5 = hashlib.md5(first_slice).hexdigest()

        with file_path.open("rb") as handle:
            handle.seek(range_start)
            hashed_bytes = 0
            while True:
                chunk = handle.read(min(self.upload_chunk_size(), file_size - hashed_bytes))
                if not chunk:
                    break
                content_md5.update(chunk)
                block_list.append(hashlib.md5(chunk).hexdigest())
                hashed_bytes += len(chunk)
                self._report_upload_progress(
                    progress_callback,
                    phase="hashing",
                    label=label or str(file_path),
                    transferred_bytes=hashed_bytes,
                    total_bytes=file_size,
                    delta_bytes=len(chunk),
                )

        if file_size == 0:
            content_md5.update(b"")

        return UploadDigestPlan(
            size=file_size,
            content_md5=content_md5.hexdigest(),
            slice_md5=first_slice_md5,
            block_list=block_list,
            local_ctime=int(stat.st_ctime),
            local_mtime=int(stat.st_mtime),
        )

    def _normalize_missing_parts(self, payload: Any) -> list[int]:
        if payload in (None, ""):
            return []
        if isinstance(payload, list):
            return [int(item) for item in payload]
        if isinstance(payload, str):
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                return []
            if isinstance(data, list):
                return [int(item) for item in data]
        return []

    def _upload_part(
        self,
        *,
        server_url: str,
        remote_path: str,
        upload_id: str,
        part_index: int,
        file_path: Path,
        progress_callback: UploadProgressCallback | None = None,
        total_bytes: int = 0,
        transferred_bytes: int = 0,
        label: str | None = None,
        byte_range: tuple[int, int] | None = None,
        session: requests.Session | None = None,
    ) -> dict[str, Any]:
        range_start, range_size = self._normalize_byte_range(file_path, byte_range)
        with file_path.open("rb") as handle:
            chunk_size = self.upload_chunk_size()
            remaining = max(0, range_size - (part_index * chunk_size))
            part_size = min(chunk_size, remaining)
            reader = _RangeReader(
                handle,
                start=range_start + (part_index * chunk_size),
                length=part_size,
            )
            state = {"sent": int(transferred_bytes)}
            wrapped = _ProgressReader(
                reader,
                callback=lambda size: (
                    state.__setitem__("sent", state["sent"] + size),
                    self._report_upload_progress(
                        progress_callback,
                        phase="uploading",
                        label=label or remote_path,
                        transferred_bytes=state["sent"],
                        total_bytes=total_bytes,
                        delta_bytes=size,
                    ),
                )[-1],
            )

            return self._request_json(
                "POST",
                f"{server_url.rstrip('/')}/rest/2.0/pcs/superfile2",
                params={
                    "method": "upload",
                    "access_token": self._access_token(),
                    "type": "tmpfile",
                    "path": remote_path,
                    "uploadid": upload_id,
                    "partseq": part_index,
                },
                files={"file": (file_path.name, wrapped)},
                session=session,
                timeout=DEFAULT_UPLOAD_TIMEOUT,
                retries=DEFAULT_UPLOAD_RETRIES,
                retry_backoff=DEFAULT_UPLOAD_RETRY_BACKOFF,
            )

    def _normalize_byte_range(
        self,
        file_path: Path,
        byte_range: tuple[int, int] | None = None,
    ) -> tuple[int, int]:
        file_size = int(file_path.stat().st_size)
        if byte_range is None:
            return 0, file_size
        start, length = int(byte_range[0]), int(byte_range[1])
        if start < 0 or length < 0 or start + length > file_size:
            raise ConfigurationError(f"Upload byte range is invalid for file: {file_path}")
        return start, length

    def _build_volume_remote_path(self, remote_path: str, *, index: int, total: int, width: int) -> str:
        normalized = self.normalize_remote_path(remote_path)
        suffix = f".{index + 1:0{width}d}"
        return self.normalize_remote_path(f"{normalized}{suffix}")

    def _build_volume_bundle_dir(self, remote_path: str) -> str:
        normalized = self.normalize_remote_path(remote_path)
        return self.normalize_remote_path(f"{normalized}.parts")

    def _build_extract_script(self, base_name: str, volume_specs: list[UploadVolumeSpec]) -> str:
        quoted_name = json.dumps(base_name, ensure_ascii=False)
        part_names = [Path(spec.target).name for spec in volume_specs]
        quoted_parts = " ".join(json.dumps(name, ensure_ascii=False) for name in part_names)
        return (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n\n"
            "SCRIPT_DIR=$(cd -- \"$(dirname -- \"$0\")\" && pwd)\n"
            f"OUTPUT_NAME={quoted_name}\n"
            "cd \"$SCRIPT_DIR\"\n\n"
            f"cat {quoted_parts} > \"$OUTPUT_NAME\"\n"
            "echo \"Restored to $SCRIPT_DIR/$OUTPUT_NAME\"\n\n"
            "extract_archive() {\n"
            "  local archive=\"$1\"\n"
            "  case \"$archive\" in\n"
            "    *.tar)\n"
            "      tar -xf \"$archive\"\n"
            "      ;;\n"
            "    *.tar.gz|*.tgz)\n"
            "      tar -xzf \"$archive\"\n"
            "      ;;\n"
            "    *.tar.bz2|*.tbz2)\n"
            "      tar -xjf \"$archive\"\n"
            "      ;;\n"
            "    *.tar.xz|*.txz)\n"
            "      tar -xJf \"$archive\"\n"
            "      ;;\n"
            "    *.zip)\n"
            "      command -v unzip >/dev/null 2>&1 || { echo \"unzip not found\" >&2; return 1; }\n"
            "      unzip -o \"$archive\"\n"
            "      ;;\n"
            "    *.7z)\n"
            "      command -v 7z >/dev/null 2>&1 || { echo \"7z not found\" >&2; return 1; }\n"
            "      7z x -y \"$archive\"\n"
            "      ;;\n"
            "    *)\n"
            "      echo \"Skip auto-extract for unsupported archive type: $archive\"\n"
            "      return 2\n"
            "      ;;\n"
            "  esac\n"
            "}\n\n"
            "if extract_archive \"$OUTPUT_NAME\"; then\n"
            f"  rm -f {quoted_parts}\n"
            "  echo \"Extraction completed. Parts deleted, archive kept: $SCRIPT_DIR/$OUTPUT_NAME\"\n"
            "else\n"
            "  status=$?\n"
            "  if [ \"$status\" -eq 2 ]; then\n"
            "    echo \"Archive restored. Parts kept because auto-extract is not supported for this file type.\"\n"
            "    exit 0\n"
            "  fi\n"
            "  echo \"Extraction failed. Parts kept for safety, archive kept: $SCRIPT_DIR/$OUTPUT_NAME\" >&2\n"
            "  exit \"$status\"\n"
            "fi\n"
        )

    def _rtype_from_policy(self, policy: str) -> int:
        policy = (policy or "").strip().lower()
        mapping = {
            "fail": 0,
            "rename": 1,
            "smart": 2,
            "overwrite": 3,
        }
        return mapping.get(policy, 3)

    def _ondup_from_policy(self, policy: str) -> str:
        policy = (policy or "").strip().lower()
        mapping = {
            "fail": "fail",
            "rename": "newcopy",
            "smart": "newcopy",
            "overwrite": "overwrite",
        }
        return mapping.get(policy, "overwrite")

    def _base_headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.config.user_agent or "pan.baidu.com",
        }

    def _download_headers(self) -> dict[str, str]:
        headers = self._base_headers()
        headers["Host"] = urlparse(PCS_BASE_URL).netloc
        return headers
