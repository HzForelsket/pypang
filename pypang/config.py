from __future__ import annotations

import json
import os
import posixpath
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_REDIRECT_URI = "oob"
DEFAULT_APP_HOME_DIRNAME = ".pypang"
DEFAULT_LEGACY_CONFIG_PATH = Path("config.json")
DEFAULT_MEMBERSHIP_TIER = "free"
DEFAULT_SINGLE_FILE_DOWNLOAD_WORKERS = 4
DEFAULT_UPLOAD_VOLUME_WORKERS = 4


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw.strip())


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", ""}
    return bool(value)


def user_home_dir() -> Path:
    return Path.home() / DEFAULT_APP_HOME_DIRNAME


def default_runtime_state_path() -> Path:
    return user_home_dir() / "state.json"


def default_runtime_temp_dir() -> Path:
    return user_home_dir() / "tmp"


def runtime_state_path() -> Path:
    raw = os.getenv("BAIDUPANWEB_STATE_PATH")
    return Path(raw).expanduser() if raw else default_runtime_state_path()


def runtime_temp_dir() -> Path:
    raw = os.getenv("BAIDUPANWEB_TEMP_DIR")
    return Path(raw).expanduser() if raw else default_runtime_temp_dir()


def legacy_config_path() -> Path:
    raw = os.getenv("BAIDUPANWEB_LEGACY_CONFIG")
    return Path(raw).expanduser() if raw else DEFAULT_LEGACY_CONFIG_PATH


@dataclass(slots=True)
class AppConfig:
    app_key: str = ""
    secret_key: str = ""
    app_id: str = ""
    app_name: str = ""
    app_root: str = ""
    redirect_uri: str = DEFAULT_REDIRECT_URI
    listen_host: str = "127.0.0.1"
    listen_port: int = 8080
    user_agent: str = "pan.baidu.com"
    enforce_app_root: bool = True
    scope: str = "basic,netdisk"
    membership_tier: str = DEFAULT_MEMBERSHIP_TIER
    upload_chunk_mb: int = 0
    cli_download_workers: int = 0
    web_download_workers: int = 0
    upload_volume_workers: int = 0
    single_file_parallel_enabled: bool = True
    single_file_download_workers: int = 0

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "AppConfig":
        payload = payload or {}
        listen_port = payload.get("listen_port", 8080)
        if isinstance(listen_port, str) and listen_port.strip():
            listen_port = int(listen_port)
        upload_chunk_mb = payload.get("upload_chunk_mb", 0)
        cli_download_workers = payload.get("cli_download_workers", 0)
        web_download_workers = payload.get("web_download_workers", 0)
        upload_volume_workers = payload.get("upload_volume_workers", 0)
        single_file_download_workers = payload.get("single_file_download_workers", 0)
        if isinstance(upload_chunk_mb, str) and upload_chunk_mb.strip():
            upload_chunk_mb = int(upload_chunk_mb)
        if isinstance(cli_download_workers, str) and cli_download_workers.strip():
            cli_download_workers = int(cli_download_workers)
        if isinstance(web_download_workers, str) and web_download_workers.strip():
            web_download_workers = int(web_download_workers)
        if isinstance(upload_volume_workers, str) and upload_volume_workers.strip():
            upload_volume_workers = int(upload_volume_workers)
        if isinstance(single_file_download_workers, str) and single_file_download_workers.strip():
            single_file_download_workers = int(single_file_download_workers)

        return cls(
            app_key=str(payload.get("app_key", "")).strip(),
            secret_key=str(payload.get("secret_key", "")).strip(),
            app_id=str(payload.get("app_id", "")).strip(),
            app_name=str(payload.get("app_name", "")).strip(),
            app_root=str(payload.get("app_root", "")).strip(),
            redirect_uri=str(payload.get("redirect_uri", DEFAULT_REDIRECT_URI)).strip()
            or DEFAULT_REDIRECT_URI,
            listen_host=str(payload.get("listen_host", "127.0.0.1")).strip() or "127.0.0.1",
            listen_port=int(listen_port),
            user_agent=str(payload.get("user_agent", "pan.baidu.com")).strip()
            or "pan.baidu.com",
            enforce_app_root=bool(payload.get("enforce_app_root", True)),
            scope=str(payload.get("scope", "basic,netdisk")).strip() or "basic,netdisk",
            membership_tier=str(payload.get("membership_tier", DEFAULT_MEMBERSHIP_TIER)).strip().lower()
            or DEFAULT_MEMBERSHIP_TIER,
            upload_chunk_mb=max(0, int(upload_chunk_mb)),
            cli_download_workers=max(0, int(cli_download_workers)),
            web_download_workers=max(0, int(web_download_workers)),
            upload_volume_workers=max(0, int(upload_volume_workers)),
            single_file_parallel_enabled=_coerce_bool(payload.get("single_file_parallel_enabled"), True),
            single_file_download_workers=max(0, int(single_file_download_workers)),
        )

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls.from_dict(
            {
                "app_key": os.getenv("BAIDUPANWEB_APP_KEY", ""),
                "secret_key": os.getenv("BAIDUPANWEB_SECRET_KEY", ""),
                "app_id": os.getenv("BAIDUPANWEB_APP_ID", ""),
                "app_name": os.getenv("BAIDUPANWEB_APP_NAME", ""),
                "app_root": os.getenv("BAIDUPANWEB_APP_ROOT", ""),
                "redirect_uri": os.getenv("BAIDUPANWEB_REDIRECT_URI", DEFAULT_REDIRECT_URI),
                "listen_host": os.getenv("BAIDUPANWEB_LISTEN_HOST", "127.0.0.1"),
                "listen_port": os.getenv("BAIDUPANWEB_LISTEN_PORT", "8080"),
                "user_agent": os.getenv("BAIDUPANWEB_USER_AGENT", "pan.baidu.com"),
                "enforce_app_root": _env_bool("BAIDUPANWEB_ENFORCE_APP_ROOT", True),
                "scope": os.getenv("BAIDUPANWEB_SCOPE", "basic,netdisk"),
                "membership_tier": os.getenv("BAIDUPANWEB_MEMBERSHIP_TIER", DEFAULT_MEMBERSHIP_TIER),
                "upload_chunk_mb": _env_int("BAIDUPANWEB_UPLOAD_CHUNK_MB", 0),
                "cli_download_workers": _env_int("BAIDUPANWEB_CLI_DOWNLOAD_WORKERS", 0),
                "web_download_workers": _env_int("BAIDUPANWEB_WEB_DOWNLOAD_WORKERS", 0),
                "upload_volume_workers": _env_int("BAIDUPANWEB_UPLOAD_VOLUME_WORKERS", 0),
                "single_file_parallel_enabled": _env_bool("BAIDUPANWEB_SINGLE_FILE_PARALLEL_ENABLED", True),
                "single_file_download_workers": _env_int("BAIDUPANWEB_SINGLE_FILE_DOWNLOAD_WORKERS", 0),
            }
        )

    @classmethod
    def from_legacy_file(cls, path: Path | None = None) -> "AppConfig":
        candidate = Path(path or legacy_config_path())
        if not candidate.exists():
            return cls()
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return cls()
        return cls.from_dict(
            {
                "app_key": payload.get("AppKey", ""),
                "secret_key": payload.get("SecretKey", ""),
                "app_name": payload.get("AppName", "") or payload.get("app_name", ""),
                "app_root": payload.get("AppRoot", "")
                or payload.get("app_root", "")
                or payload.get("AppPcsPath", "")
                or payload.get("app_pcs_path", ""),
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def merge(self, overrides: "AppConfig") -> "AppConfig":
        data = self.to_dict()
        for key, value in overrides.to_dict().items():
            if isinstance(value, bool):
                data[key] = value
                continue
            if value not in (None, "", []):
                data[key] = value
        return AppConfig.from_dict(data)

    def resolved_app_root(self) -> str:
        raw = self.app_root.strip()
        if not raw and self.app_name.strip():
            raw = f"/apps/{self.app_name.strip()}"
        if not raw:
            return ""
        if not raw.startswith("/"):
            raw = f"/{raw}"
        normalized = posixpath.normpath(raw)
        return normalized if normalized.startswith("/") else f"/{normalized}"

    def is_ready_for_auth(self) -> bool:
        return bool(self.app_key and self.secret_key and self.redirect_uri)

    def is_ready_for_api(self) -> bool:
        return bool(self.is_ready_for_auth() and self.resolved_app_root())

    def normalized_membership_tier(self) -> str:
        candidate = self.membership_tier.strip().lower()
        if candidate in {"vip", "svip"}:
            return candidate
        return DEFAULT_MEMBERSHIP_TIER

    def max_upload_chunk_mb(self) -> int:
        tier = self.normalized_membership_tier()
        if tier == "svip":
            return 32
        if tier == "vip":
            return 16
        return 4

    def max_download_workers(self) -> int:
        return 8

    def max_upload_volume_workers(self) -> int:
        return 8

    def effective_upload_chunk_mb(self) -> int:
        if self.upload_chunk_mb <= 0:
            return self.max_upload_chunk_mb()
        return max(1, min(self.upload_chunk_mb, self.max_upload_chunk_mb()))

    def effective_cli_download_workers(self) -> int:
        if self.cli_download_workers <= 0:
            return self.max_download_workers()
        return max(1, min(self.cli_download_workers, self.max_download_workers()))

    def effective_web_download_workers(self) -> int:
        if self.web_download_workers <= 0:
            return self.max_download_workers()
        return max(1, min(self.web_download_workers, self.max_download_workers()))

    def effective_upload_volume_workers(self) -> int:
        if self.upload_volume_workers <= 0:
            return DEFAULT_UPLOAD_VOLUME_WORKERS
        return max(1, min(self.upload_volume_workers, self.max_upload_volume_workers()))

    def effective_single_file_download_workers(self) -> int:
        if not self.single_file_parallel_enabled:
            return 1
        if self.single_file_download_workers <= 0:
            return DEFAULT_SINGLE_FILE_DOWNLOAD_WORKERS
        return max(1, min(self.single_file_download_workers, self.max_download_workers()))
