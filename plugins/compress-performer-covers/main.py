import base64
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError

from avif_codec import (
    MAX_QUALITY,
    compress_cover,
    resolve_executable,
    validate_ffmpeg,
)
from plugin_runtime import (
    emit_info,
    emit_progress,
    emit_warn,
    load_plugin_input,
    read_api_key,
)
from stash_api import (
    StashClient,
    configure_plugin,
    find_performer,
    find_performers,
    get_runtime_config,
    update_performer_image,
)

DEFAULT_TARGET_KB = 100
DEFAULT_MAX_WIDTH = 720
DEFAULT_MIN_QUALITY = 60
DEFAULT_CONCURRENCY = 3
DEFAULT_LIMIT = 0

DEFAULT_SETTINGS: dict[str, Any] = {
    "target_kb": DEFAULT_TARGET_KB,
    "max_width": DEFAULT_MAX_WIDTH,
    "min_quality": DEFAULT_MIN_QUALITY,
    "concurrency": DEFAULT_CONCURRENCY,
    "limit": DEFAULT_LIMIT,
    "ffmpeg_path": "",
}


@dataclass(frozen=True)
class Options:
    apply: bool
    target_bytes: int
    max_width: int
    min_quality: int
    concurrency: int
    limit: int
    ffmpeg_path: str | None


def resolve_graphql_url(server_connection: dict[str, Any]) -> str:
    explicit_url = str(server_connection.get("GraphQLURL") or "").strip()
    if explicit_url:
        return explicit_url

    scheme = server_connection.get("Scheme") or "http"
    host = server_connection.get("Host") or "127.0.0.1"
    if host == "0.0.0.0":
        host = "127.0.0.1"
    port = server_connection.get("Port") or 9999
    return f"{scheme}://{host}:{port}/graphql"


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def parse_int(
    settings: dict[str, Any],
    key: str,
    default: int,
    minimum: int,
    maximum: int | None = None,
) -> int:
    raw = settings.get(key)
    if raw in (None, ""):
        return default

    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Setting {key} must be an integer; received: {raw}") from exc

    if value < minimum or (maximum is not None and value > maximum):
        allowed = (
            f"between {minimum} and {maximum}" if maximum is not None else f"at least {minimum}"
        )
        raise ValueError(f"Setting {key} must be {allowed}; received: {value}")
    return value


def load_options(args: dict[str, Any], settings: dict[str, Any]) -> Options:
    target_kb = parse_int(settings, "target_kb", DEFAULT_TARGET_KB, 1)
    ffmpeg_path = str(settings.get("ffmpeg_path") or "").strip() or None
    return Options(
        apply=parse_bool(args.get("apply")),
        target_bytes=target_kb * 1024,
        max_width=parse_int(settings, "max_width", DEFAULT_MAX_WIDTH, 160),
        min_quality=parse_int(settings, "min_quality", DEFAULT_MIN_QUALITY, 1, MAX_QUALITY),
        concurrency=parse_int(settings, "concurrency", DEFAULT_CONCURRENCY, 1, 16),
        limit=parse_int(settings, "limit", DEFAULT_LIMIT, 0),
        ffmpeg_path=ffmpeg_path,
    )


def format_bytes(size: int) -> str:
    return f"{size / 1024:.1f} KiB"


def select_performers(
    client: StashClient,
    args: dict[str, Any],
    limit: int,
) -> tuple[list[dict[str, Any]], bool]:
    hook_context = args.get("hookContext")
    if not hook_context:
        performers = find_performers(client)
        return (performers[:limit] if limit else performers), False

    input_fields = set(hook_context.get("inputFields") or [])
    if "image" not in input_fields:
        emit_info("The performer update did not include the image field; skipping compression")
        return [], True

    performer_id = hook_context.get("id")
    if performer_id is None:
        raise RuntimeError("Performer.Update.Post hook did not provide a performer ID")

    performer = find_performer(client, str(performer_id))
    if not performer:
        raise RuntimeError(f"Performer not found: {performer_id}")
    return [performer], True


def process_performer(
    client: StashClient,
    performer: dict[str, Any],
    options: Options,
    ffmpeg_path: str,
    ffprobe_path: str,
) -> dict[str, Any]:
    image_path = performer.get("image_path")
    if not image_path:
        return {"status": "skipped", "reason": "No cover"}

    remote_size = client.image_size(str(image_path))
    if remote_size is not None and remote_size <= options.target_bytes:
        return {
            "status": "skipped",
            "reason": f"Already below threshold at {format_bytes(remote_size)}",
        }

    original = client.download_image(str(image_path))
    if len(original) <= options.target_bytes:
        return {
            "status": "skipped",
            "reason": f"Already below threshold at {format_bytes(len(original))}",
        }

    compressed = compress_cover(
        source=original,
        ffmpeg_path=ffmpeg_path,
        ffprobe_path=ffprobe_path,
        target_bytes=options.target_bytes,
        min_quality=options.min_quality,
        max_width=options.max_width,
    )
    if options.apply:
        encoded = base64.b64encode(compressed.data).decode("ascii")
        update_performer_image(
            client,
            str(performer["id"]),
            f"data:image/avif;base64,{encoded}",
        )

    return {
        "status": "updated" if options.apply else "preview",
        "original_bytes": len(original),
        "compressed_bytes": len(compressed.data),
        "quality": compressed.quality,
        "width": compressed.width,
        "height": compressed.height,
    }


def run() -> dict[str, Any]:
    plugin_input = load_plugin_input()
    server_connection = plugin_input.get("server_connection") or {}
    args = plugin_input.get("args") or {}
    client = StashClient(
        graphql_url=resolve_graphql_url(server_connection),
        api_key=read_api_key(server_connection.get("Dir")),
        session_cookie=server_connection.get("SessionCookie"),
    )

    if parse_bool(args.get("reset_settings")):
        saved_settings = configure_plugin(client, DEFAULT_SETTINGS)
        emit_info("Plugin settings were reset to their built-in defaults")
        return {
            "error": None,
            "output": {
                "settings": saved_settings,
            },
        }

    runtime = get_runtime_config(client)
    options = load_options(args, runtime["settings"])
    performers, is_hook = select_performers(client, args, options.limit)
    if not performers:
        return {
            "error": None,
            "output": {
                "apply": options.apply,
                "summary": {"updated": 0, "preview": 0, "skipped": 0, "failed": 0},
                "results": [],
            },
        }

    system = runtime["system"]
    ffmpeg_path = resolve_executable(options.ffmpeg_path or system.get("ffmpegPath"), "ffmpeg")
    ffprobe_path = resolve_executable(system.get("ffprobePath"), "ffprobe")
    validate_ffmpeg(ffmpeg_path)

    mode = "write to Stash" if options.apply else "preview without writing"
    source = "Performer.Update.Post hook" if is_hook else "manual task"
    emit_info(
        f"Processing {len(performers)} performers; source={source}; mode={mode}; "
        f"target={format_bytes(options.target_bytes)}; max_width={options.max_width}; "
        f"min_quality={options.min_quality}; concurrency={options.concurrency}"
    )
    emit_info(f"FFmpeg={ffmpeg_path}")
    emit_progress(0.0)

    results: list[dict[str, Any]] = []
    summary = {"updated": 0, "preview": 0, "skipped": 0, "failed": 0}
    progress_lock = threading.Lock()
    completed = 0

    with ThreadPoolExecutor(max_workers=options.concurrency) as executor:
        futures = {
            executor.submit(
                process_performer,
                client,
                performer,
                options,
                ffmpeg_path,
                ffprobe_path,
            ): performer
            for performer in performers
        }
        for future in as_completed(futures):
            performer = futures[future]
            try:
                result = future.result()
                status = result["status"]
                summary[status] += 1
                results.append({"id": performer["id"], "name": performer["name"], **result})
                if status == "skipped":
                    emit_info(f"Skipped {performer['name']}: {result['reason']}")
                else:
                    action = "Updated" if status == "updated" else "Compressible"
                    emit_info(
                        f"{action} {performer['name']}: "
                        f"{format_bytes(result['original_bytes'])} -> "
                        f"{format_bytes(result['compressed_bytes'])}, "
                        f"quality={result['quality']}, "
                        f"{result['width']}x{result['height']}"
                    )
            except Exception as exc:  # A single failed cover must not abort the batch.
                summary["failed"] += 1
                result = {
                    "id": performer["id"],
                    "name": performer["name"],
                    "status": "failed",
                    "reason": str(exc),
                }
                results.append(result)
                emit_warn(f"Failed {performer['name']}: {exc}")
            finally:
                with progress_lock:
                    completed += 1
                    emit_progress(completed / max(len(performers), 1))

    emit_progress(1.0)
    emit_info(
        f"Finished: updated={summary['updated']}, preview={summary['preview']}, "
        f"skipped={summary['skipped']}, failed={summary['failed']}"
    )
    return {
        "error": None,
        "output": {
            "apply": options.apply,
            "ffmpeg_path": ffmpeg_path,
            "summary": summary,
            "results": results,
        },
    }


def main() -> None:
    try:
        result = run()
    except (
        HTTPError,
        URLError,
        TimeoutError,
        RuntimeError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
    ) as exc:
        print(json.dumps({"error": str(exc), "output": None}, ensure_ascii=False))
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
