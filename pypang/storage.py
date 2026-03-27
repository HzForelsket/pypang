from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .app_paths import default_app_config
from .config import AppConfig, runtime_state_path
from .errors import ConfigurationError


def config_profile_id(config: AppConfig) -> str:
    payload = "|".join(
        [
            config.app_key.strip(),
            config.secret_key.strip(),
            config.app_name.strip(),
            config.app_root.strip(),
            config.redirect_uri.strip(),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class OAuthToken:
    access_token: str = ""
    refresh_token: str = ""
    expires_in: int = 0
    scope: str = ""
    token_type: str = "bearer"
    created_at: int = 0

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "OAuthToken | None":
        payload = payload or {}
        access_token = str(payload.get("access_token", "")).strip()
        if not access_token:
            return None
        return cls(
            access_token=access_token,
            refresh_token=str(payload.get("refresh_token", "")).strip(),
            expires_in=int(payload.get("expires_in", 0) or 0),
            scope=str(payload.get("scope", "")).strip(),
            token_type=str(payload.get("token_type", "bearer")).strip() or "bearer",
            created_at=int(payload.get("created_at", 0) or 0),
        )

    @classmethod
    def from_oauth_payload(cls, payload: dict[str, Any]) -> "OAuthToken":
        return cls(
            access_token=str(payload.get("access_token", "")).strip(),
            refresh_token=str(payload.get("refresh_token", "")).strip(),
            expires_in=int(payload.get("expires_in", 0) or 0),
            scope=str(payload.get("scope", "")).strip(),
            token_type=str(payload.get("token_type", "bearer")).strip() or "bearer",
            created_at=int(time.time()),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_in": self.expires_in,
            "scope": self.scope,
            "token_type": self.token_type,
            "created_at": self.created_at,
        }

    @property
    def expires_at(self) -> int:
        if not self.created_at or not self.expires_in:
            return 0
        return self.created_at + self.expires_in

    def is_expired(self, leeway_seconds: int = 300) -> bool:
        if not self.access_token:
            return True
        if not self.expires_at:
            return False
        return int(time.time()) >= (self.expires_at - leeway_seconds)


@dataclass(slots=True)
class AppState:
    config: AppConfig
    token: OAuthToken | None = None
    tokens: dict[str, OAuthToken] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "AppState":
        payload = payload or {}
        config = AppConfig.from_dict(payload.get("config"))
        tokens_payload = payload.get("tokens") or {}
        tokens = {
            str(key): token
            for key, raw in tokens_payload.items()
            if (token := OAuthToken.from_dict(raw)) is not None
        }
        legacy_token = OAuthToken.from_dict(payload.get("token"))
        if legacy_token:
            tokens.setdefault(config_profile_id(config), legacy_token)
        token = tokens.get(config_profile_id(config))
        return cls(config=config, token=token, tokens=tokens)

    def to_dict(self) -> dict[str, Any]:
        current_key = config_profile_id(self.config)
        current_token = self.tokens.get(current_key) or self.token
        return {
            "config": self.config.to_dict(),
            "token": current_token.to_dict() if current_token else None,
            "tokens": {key: token.to_dict() for key, token in self.tokens.items()},
        }


class StateStore:
    def __init__(self, path: Path | None = None, base_config: AppConfig | None = None):
        self.path = Path(path or runtime_state_path())
        base = base_config or default_app_config()
        self.base_config = base.merge(AppConfig.from_env())

    def load(self) -> AppState:
        if self.path.exists():
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise ConfigurationError(
                    f"Failed to parse state file: {self.path}"
                ) from exc
            state = AppState.from_dict(payload)
        else:
            state = AppState(config=AppConfig())

        state.config = self.base_config.merge(state.config)
        state.token = state.tokens.get(config_profile_id(state.config))
        return state

    def save(self, state: AppState) -> AppState:
        current_key = config_profile_id(state.config)
        if state.token:
            state.tokens[current_key] = state.token
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(state.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return state

    def update_config(self, config: AppConfig, *, clear_token: bool = False) -> AppState:
        state = self.load()
        previous_key = config_profile_id(state.config)
        state.config = config
        current_key = config_profile_id(state.config)
        if clear_token:
            state.tokens.pop(current_key, None)
        if previous_key != current_key:
            state.token = state.tokens.get(current_key)
        elif clear_token:
            state.token = None
        return self.save(state)

    def update_token(self, token: OAuthToken) -> AppState:
        state = self.load()
        current_key = config_profile_id(state.config)
        state.tokens[current_key] = token
        state.token = token
        return self.save(state)

    def clear_token(self) -> AppState:
        state = self.load()
        current_key = config_profile_id(state.config)
        state.tokens.pop(current_key, None)
        state.token = None
        return self.save(state)
