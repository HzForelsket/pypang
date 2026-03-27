from __future__ import annotations

import json
import importlib.resources as resources
import posixpath
from pathlib import Path
from typing import Any

from .config import AppConfig, legacy_config_path


DEFAULT_APP_LIST_PATH = Path("app.list.json")


def _normalize_name(value: str) -> str:
    return str(value or "").strip().strip("/")


def _normalize_root(value: str) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    if not raw:
        return ""
    if not raw.startswith("/"):
        raw = f"/{raw}"
    normalized = posixpath.normpath(raw)
    return normalized if normalized.startswith("/") else f"/{normalized}"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _load_builtin_json(path: Path | None = None) -> dict[str, Any]:
    candidate = Path(path or DEFAULT_APP_LIST_PATH)
    payload = _load_json(candidate)
    if payload:
        return payload
    try:
        data = resources.files("pypang").joinpath("app.list.json").read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError):
        return {}
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return {}


def _build_choice(
    *,
    app_key: str = "",
    secret_key: str = "",
    app_name: str = "",
    app_root: str = "",
    label: str = "",
    source: str = "builtin",
    choice_id: str = "",
) -> dict[str, str] | None:
    normalized_root = _normalize_root(app_root)
    normalized_name = _normalize_name(app_name)
    if not normalized_name and normalized_root.startswith("/apps/"):
        normalized_name = normalized_root.removeprefix("/apps/").strip("/")
    if not normalized_root and normalized_name:
        normalized_root = f"/apps/{normalized_name}"
    if not normalized_root and not (str(app_key or "").strip() and str(secret_key or "").strip()):
        return None
    if not normalized_name:
        normalized_name = Path(normalized_root).name if normalized_root else label or "custom"
    return {
        "id": choice_id or normalized_name,
        "label": label or normalized_name,
        "app_key": str(app_key or "").strip(),
        "secret_key": str(secret_key or "").strip(),
        "app_name": normalized_name,
        "app_root": normalized_root,
        "source": source,
    }


def _iter_choice_payloads(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key in ("apps", "app_list", "AppList", "profiles", "Profiles"):
        value = payload.get(key)
        if isinstance(value, list):
            items.extend(item for item in value if isinstance(item, dict))
    if any(payload.get(key) for key in ("AppKey", "app_key", "AppRoot", "app_root", "AppPcsPath", "app_pcs_path")):
        items.append(payload)
    return items


def _choice_from_payload(item: dict[str, Any], *, source: str, fallback_id: str) -> dict[str, str] | None:
    return _build_choice(
        app_key=item.get("app_key", "") or item.get("AppKey", ""),
        secret_key=item.get("secret_key", "") or item.get("SecretKey", ""),
        app_name=item.get("app_name", "") or item.get("AppName", "") or item.get("name", "") or item.get("Name", ""),
        app_root=item.get("app_root", "")
        or item.get("AppRoot", "")
        or item.get("path", "")
        or item.get("Path", "")
        or item.get("AppPcsPath", "")
        or item.get("app_pcs_path", ""),
        label=item.get("label", "") or item.get("Label", "") or item.get("title", "") or item.get("Title", ""),
        source=source,
        choice_id=str(item.get("id", "") or item.get("Id", "") or fallback_id),
    )


def load_builtin_app_choices(path: Path | None = None) -> list[dict[str, str]]:
    payload = _load_builtin_json(path)
    choices: list[dict[str, str]] = []
    for index, item in enumerate(_iter_choice_payloads(payload)):
        choice = _choice_from_payload(item, source="builtin", fallback_id=f"builtin-{index}")
        if choice:
            choices.append(choice)
    return choices


def _load_custom_payload(path: Path | None = None) -> dict[str, Any]:
    return _load_json(Path(path or legacy_config_path()))


def load_custom_app_choices(path: Path | None = None) -> list[dict[str, str]]:
    payload = _load_custom_payload(path)
    if not payload:
        return []

    choices: list[dict[str, str]] = []
    for index, item in enumerate(_iter_choice_payloads(payload)):
        choice = _choice_from_payload(item, source="config", fallback_id=f"config-{index}")
        if choice:
            choices.append(choice)
    return choices


def load_available_app_choices() -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for choice in [*load_builtin_app_choices(), *load_custom_app_choices()]:
        key = (
            choice["app_key"],
            choice["secret_key"],
            choice["app_name"],
            choice["app_root"],
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(choice)
    return merged


def _find_default_choice(choices: list[dict[str, str]], payload: dict[str, Any]) -> dict[str, str] | None:
    default_value = str(
        payload.get("default")
        or payload.get("default_app")
        or payload.get("default_profile")
        or payload.get("Default")
        or ""
    ).strip()
    if default_value:
        for choice in choices:
            if default_value in {choice["id"], choice["label"], choice["app_name"], choice["app_root"]}:
                return choice
    return choices[0] if choices else None


def default_app_config() -> AppConfig:
    custom_payload = _load_custom_payload()
    custom_choices = load_custom_app_choices()
    default_choice = _find_default_choice(custom_choices, custom_payload)
    if not default_choice:
        builtin_choices = load_builtin_app_choices()
        default_choice = builtin_choices[0] if builtin_choices else None
    if not default_choice:
        return AppConfig()
    return AppConfig.from_dict(
        {
            "app_key": default_choice["app_key"],
            "secret_key": default_choice["secret_key"],
            "app_name": default_choice["app_name"],
            "app_root": default_choice["app_root"],
        }
    )
