from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path


from . import __version__
from .client import BaiduPanClient
from .config import AppConfig
from .errors import BaiduPanError
from .storage import StateStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pypang",
        description="Baidu Pan client with a matching Web UI.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    config_cmd = sub.add_parser("config", help="Show or update local configuration.")
    config_sub = config_cmd.add_subparsers(dest="config_command", required=True)
    config_sub.add_parser("show", help="Print the current configuration.")
    config_set = config_sub.add_parser("set", help="Update configuration fields.")
    config_set.add_argument("--app-key")
    config_set.add_argument("--secret-key")
    config_set.add_argument("--app-id")
    config_set.add_argument("--app-name")
    config_set.add_argument("--app-root")
    config_set.add_argument("--redirect-uri")
    config_set.add_argument("--listen-host")
    config_set.add_argument("--listen-port", type=int)
    config_set.add_argument("--user-agent")
    config_set.add_argument("--scope")
    config_set.add_argument("--membership-tier", choices=["free", "vip", "svip"])
    config_set.add_argument("--upload-chunk-mb", type=int)
    config_set.add_argument("--cli-download-workers", type=int)
    config_set.add_argument("--web-download-workers", type=int)
    config_set.add_argument("--upload-volume-workers", type=int)
    config_set.add_argument(
        "--single-file-parallel-enabled",
        dest="single_file_parallel_enabled",
        action=argparse.BooleanOptionalAction,
    )
    config_set.add_argument("--single-file-download-workers", type=int)

    auth_cmd = sub.add_parser("auth", help="Authorization helpers.")
    auth_sub = auth_cmd.add_subparsers(dest="auth_command", required=True)
    auth_sub.add_parser("url", help="Print the Baidu OAuth authorization URL.")
    auth_code = auth_sub.add_parser("code", help="Exchange an authorization code.")
    auth_code.add_argument("code")
    auth_sub.add_parser("refresh", help="Refresh the access token.")
    auth_sub.add_parser("logout", help="Remove the local access token.")

    serve_cmd = sub.add_parser("serve", help="Run the Web UI server.")
    serve_cmd.add_argument("--host")
    serve_cmd.add_argument("--port", type=int)
    serve_cmd.add_argument("--reload", action="store_true")

    sub.add_parser("info", help="Show the authorized user info.")
    sub.add_parser("quota", help="Show the current quota usage.")

    list_cmd = sub.add_parser("list", help="List files under a remote directory.")
    list_cmd.add_argument("path", nargs="?", default="/")

    ls_cmd = sub.add_parser("ls", help="Alias of list.")
    ls_cmd.add_argument("path", nargs="?", default="/")

    mkdir_cmd = sub.add_parser("mkdir", help="Create a remote directory.")
    mkdir_cmd.add_argument("path")
    mkdir_cmd.add_argument("--rename-on-conflict", action="store_true")

    upload_cmd = sub.add_parser("upload", help="Upload a local file to a remote file or directory.")
    upload_cmd.add_argument("local_path")
    upload_cmd.add_argument("remote_path", nargs="?")
    upload_cmd.add_argument(
        "--policy",
        choices=["fail", "rename", "smart", "overwrite"],
        default="overwrite",
    )
    upload_cmd.add_argument("--single-step", action="store_true")

    put_cmd = sub.add_parser("put", help="Alias of upload.")
    put_cmd.add_argument("local_path")
    put_cmd.add_argument("remote_path", nargs="?")
    put_cmd.add_argument(
        "--policy",
        choices=["fail", "rename", "smart", "overwrite"],
        default="overwrite",
    )
    put_cmd.add_argument("--single-step", action="store_true")

    download_cmd = sub.add_parser("download", help="Download a remote file.")
    download_cmd.add_argument("remote_path")
    download_cmd.add_argument("destination", nargs="?")
    download_cmd.add_argument("--no-resume", action="store_true")
    download_cmd.add_argument(
        "--single-file-parallel",
        dest="single_file_parallel",
        action=argparse.BooleanOptionalAction,
        default=None,
    )

    get_cmd = sub.add_parser("get", help="Alias of download.")
    get_cmd.add_argument("remote_path")
    get_cmd.add_argument("destination", nargs="?")
    get_cmd.add_argument("--no-resume", action="store_true")
    get_cmd.add_argument(
        "--single-file-parallel",
        dest="single_file_parallel",
        action=argparse.BooleanOptionalAction,
        default=None,
    )

    rename_cmd = sub.add_parser("rename", help="Rename a remote file or folder.")
    rename_cmd.add_argument("path")
    rename_cmd.add_argument("new_name")

    move_cmd = sub.add_parser("move", help="Move a remote file or folder.")
    move_cmd.add_argument("path")
    move_cmd.add_argument("destination_dir")
    move_cmd.add_argument("--new-name")

    mv_cmd = sub.add_parser("mv", help="Alias of move.")
    mv_cmd.add_argument("path")
    mv_cmd.add_argument("destination_dir")
    mv_cmd.add_argument("--new-name")

    delete_cmd = sub.add_parser("delete", help="Delete one or more remote paths.")
    delete_cmd.add_argument("paths", nargs="+")

    rm_cmd = sub.add_parser("rm", help="Alias of delete.")
    rm_cmd.add_argument("paths", nargs="+")

    sub.add_parser("whoami", help="Alias of info.")

    return parser


def _print_json(payload):
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _format_size(num_bytes: int) -> str:
    value = float(max(0, int(num_bytes)))
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)}{unit}"
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}TB"


def _format_mtime(timestamp: int) -> str:
    if not timestamp:
        return "-"
    return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M")


def _print_listing(client: BaiduPanClient, payload: dict) -> None:
    items = list(payload.get("list") or [])
    cwd = client.display_path(str(payload.get("cwd") or "/"))
    print(f"Directory: {cwd}")
    if not items:
        print("(empty)")
        return

    rows: list[tuple[str, str, str, str]] = []
    for item in items:
        is_dir = bool(item.get("isdir"))
        item_type = "DIR" if is_dir else "FILE"
        size = "-" if is_dir else _format_size(int(item.get("size", 0) or 0))
        mtime = _format_mtime(int(item.get("server_mtime", 0) or item.get("local_mtime", 0) or 0))
        name = str(item.get("server_filename") or Path(str(item.get("path") or "")).name or "")
        rows.append((item_type, size, mtime, name))

    type_width = max(len(row[0]) for row in rows)
    size_width = max(len(row[1]) for row in rows)
    time_width = max(len(row[2]) for row in rows)
    for item_type, size, mtime, name in rows:
        print(f"{item_type:<{type_width}}  {size:>{size_width}}  {mtime:<{time_width}}  {name}")


class _CliProgressRenderer:
    def __init__(self, action: str):
        self.action = action
        self._last_render_at = 0.0
        self._last_line_length = 0
        self._current_value = 0
        self._last_foreground_event: dict | None = None
        self._prepare_event: dict | None = None
        self._last_speed_phase = ""
        self._last_speed_volume_index = 0
        self._last_speed_sample_at = time.time()
        self._last_speed_value = 0
        self._window_speed_bps = 0.0

    def update(self, event: dict) -> None:
        stream = str(event.get("stream") or "foreground")
        if stream == "prepare":
            self._prepare_event = dict(event)
        else:
            self._last_foreground_event = dict(event)
            foreground_volume_index = int(event.get("volume_index", 0) or 0)
            prepare_volume_index = int((self._prepare_event or {}).get("volume_index", 0) or 0)
            if prepare_volume_index and prepare_volume_index <= foreground_volume_index:
                self._prepare_event = None

        display_event = self._last_foreground_event or dict(event)
        phase = str(display_event.get("phase") or "")
        label = str(display_event.get("label") or self.action)
        volume_index = int(display_event.get("volume_index", 0) or 0)
        volume_count = int(display_event.get("volume_count", 0) or 0)
        active_uploads = int(display_event.get("active_uploads", 0) or 0)
        completed_volumes = int(display_event.get("completed_volumes", 0) or 0)
        total = int(
            display_event.get("download_total_bytes", 0)
            or display_event.get("verify_total_bytes", 0)
            or display_event.get("total_bytes", 0)
            or 0
        )
        if "downloaded_bytes" in display_event:
            value = int(display_event.get("downloaded_bytes", 0) or 0)
            delta = int(display_event.get("download_delta_bytes", 0) or 0)
        elif "verify_bytes" in display_event and phase == "verifying":
            value = int(display_event.get("verify_bytes", 0) or 0)
            delta = 0
        else:
            if stream != "prepare" and bool(display_event.get("incremental")):
                self._current_value += int(display_event.get("delta_bytes", 0) or 0)
            elif stream != "prepare":
                self._current_value = int(display_event.get("transferred_bytes", 0) or 0)
            else:
                self._current_value = int(display_event.get("transferred_bytes", 0) or 0)
            value = self._current_value
            delta = int(display_event.get("delta_bytes", 0) or 0)

        now = time.time()
        if stream != "prepare":
            if phase != self._last_speed_phase or volume_index != self._last_speed_volume_index:
                self._last_speed_phase = phase
                self._last_speed_volume_index = volume_index
                self._last_speed_sample_at = now
                self._last_speed_value = value
                self._window_speed_bps = 0.0
            elif phase in {"hashing", "uploading"}:
                elapsed = now - self._last_speed_sample_at
                if elapsed >= 2.0:
                    delta_value = max(0, value - self._last_speed_value)
                    self._window_speed_bps = delta_value / max(elapsed, 1e-6)
                    self._last_speed_sample_at = now
                    self._last_speed_value = value
        speed = self._window_speed_bps

        render_interval = 2.0 if phase in {"hashing", "uploading"} else 0.2
        should_render = phase == "completed" or (now - self._last_render_at) >= render_interval
        if not should_render:
            return

        if phase == "completed":
            line = f"{self.action}: completed  {_format_size(value)}  {label}"
        else:
            percent = f"{(value / total * 100):5.1f}%" if total > 0 else "  ---%"
            speed_text = f"{_format_size(int(speed))}/s" if speed > 0 else "--"
            phase_text = {
                "hashing": "hashing",
                "uploading": "uploading",
                "downloading": "downloading",
                "verifying": "verifying",
            }.get(phase, phase or self.action)
            if phase == "uploading" and volume_count > 1 and active_uploads > 1:
                phase_text = f"uploading {active_uploads}x"
            elif volume_count > 1 and volume_index > 0:
                phase_text = f"{phase_text} volume {volume_index}/{volume_count}"
            line = f"{self.action}: {phase_text:<22} {percent}  {_format_size(value)}/{_format_size(total) if total else '?'}  {speed_text:<10}  {label}"
            if volume_count > 1 and completed_volumes > 0:
                line += f" | done {completed_volumes}/{volume_count}"
            if self._prepare_event:
                prepare_index = int(self._prepare_event.get("volume_index", 0) or 0)
                prepare_count = int(self._prepare_event.get("volume_count", 0) or 0)
                prepare_total = int(self._prepare_event.get("total_bytes", 0) or 0)
                prepare_value = int(self._prepare_event.get("transferred_bytes", 0) or 0)
                if prepare_index > volume_index:
                    prepare_percent = (
                        f"{(prepare_value / prepare_total * 100):4.0f}%"
                        if prepare_total > 0
                        else "--%"
                    )
                    line += f" | preparing volume {prepare_index}/{prepare_count} {prepare_percent}"

        padding = max(0, self._last_line_length - len(line))
        sys.stdout.write("\r" + line + (" " * padding))
        sys.stdout.flush()
        self._last_line_length = len(line)
        self._last_render_at = now

    def finish(self) -> None:
        if self._last_line_length:
            sys.stdout.write("\n")
            sys.stdout.flush()
            self._last_line_length = 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    if argv is not None and not argv:
        parser.print_help()
        return 0
    if argv is None and len(sys.argv) <= 1:
        parser.print_help()
        return 0
    args = parser.parse_args(argv)
    store = StateStore()
    client = BaiduPanClient(store=store)

    try:
        if args.command == "config":
            return _handle_config(store, args)
        if args.command == "auth":
            return _handle_auth(store, client, args)
        if args.command == "serve":
            return _handle_serve(store, args)
        if args.command == "whoami":
            _print_json(client.get_user_info())
            return 0
        if args.command == "info":
            _print_json(client.get_user_info())
            return 0
        if args.command == "quota":
            _print_json(client.get_quota())
            return 0
        if args.command in {"list", "ls"}:
            _print_listing(client, client.list_files(args.path))
            return 0
        if args.command == "mkdir":
            _print_json(
                client.create_folder(args.path, rename_on_conflict=args.rename_on_conflict)
            )
            return 0
        if args.command in {"upload", "put"}:
            progress = _CliProgressRenderer("upload")
            result = client.upload_file(
                args.local_path,
                args.remote_path,
                policy=args.policy,
                prefer_single_step=args.single_step,
                progress_callback=progress.update,
            )
            progress.finish()
            if result.get("is_multi_volume"):
                print(
                    f"Uploaded: {args.local_path} -> "
                    f"{client.display_path(str(result.get('path') or args.remote_path or ''))} "
                    f"as {int(result.get('volume_count') or 0)} volumes"
                )
                for volume in result.get("volumes", []):
                    print(f"  - {client.display_path(str(volume.get('path') or ''))}")
            else:
                print(
                    f"Uploaded: {args.local_path} -> "
                    f"{client.display_path(str(result.get('path') or args.remote_path or ''))}"
                )
            return 0
        if args.command in {"download", "get"}:
            progress = _CliProgressRenderer("download")
            target = client.download_file(
                args.remote_path,
                args.destination,
                resume=not args.no_resume,
                progress_callback=progress.update,
                single_file_parallel=args.single_file_parallel,
            )
            progress.finish()
            print(target)
            return 0
        if args.command == "rename":
            _print_json(client.rename(args.path, args.new_name))
            return 0
        if args.command in {"move", "mv"}:
            _print_json(client.move(args.path, args.destination_dir, new_name=args.new_name))
            return 0
        if args.command in {"delete", "rm"}:
            _print_json(client.delete(args.paths))
            return 0
    except BaiduPanError as exc:
        print(f"Error: {exc}")
        return 1

    parser.print_help()
    return 1


def _handle_config(store: StateStore, args) -> int:
    state = store.load()
    if args.config_command == "show":
        _print_json(state.to_dict())
        return 0

    data = state.config.to_dict()
    changed = False
    for field in (
        "app_key",
        "secret_key",
        "app_id",
        "app_name",
        "app_root",
        "redirect_uri",
        "listen_host",
        "listen_port",
        "user_agent",
        "scope",
        "membership_tier",
        "upload_chunk_mb",
        "cli_download_workers",
        "web_download_workers",
        "upload_volume_workers",
        "single_file_parallel_enabled",
        "single_file_download_workers",
    ):
        value = getattr(args, field, None)
        if value not in (None, ""):
            data[field] = value
            changed = True

    if not changed:
        print("No values changed.")
        return 0

    auth_fields = {"app_key", "secret_key", "app_id", "app_name", "app_root", "redirect_uri", "scope"}
    clear_token = any(
        getattr(args, field, None) not in (None, "")
        for field in auth_fields
    )
    store.update_config(AppConfig.from_dict(data), clear_token=clear_token)
    print(store.path)
    return 0




def _handle_serve(store: StateStore, args) -> int:
    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        raise BaiduPanError("uvicorn is required for the Web UI. Install dependencies with pip install -r requirements.txt.") from exc

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    config = store.load().config
    host = args.host or config.listen_host or "127.0.0.1"
    port = args.port or config.listen_port or 8080
    uvicorn.run(
        "pypang.app:app",
        host=host,
        port=int(port),
        reload=bool(args.reload),
    )
    return 0


def _handle_auth(store: StateStore, client: BaiduPanClient, args) -> int:
    if args.auth_command == "url":
        print(client.build_authorize_url())
        return 0
    if args.auth_command == "code":
        _print_json(client.exchange_code(args.code).to_dict())
        return 0
    if args.auth_command == "refresh":
        _print_json(client.refresh_access_token().to_dict())
        return 0
    if args.auth_command == "logout":
        store.clear_token()
        print("Token cleared.")
        return 0
    return 1
