import json
import sys
from typing import Any
from urllib.error import HTTPError, URLError

from plugin_runtime import emit_info, emit_warn, load_plugin_input, read_api_key
from stash_api import StashClient
from sync_logic import collect_targets, load_tag_taxonomy, process_targets, select_target_galleries


def run() -> dict[str, Any]:
    plugin_input = load_plugin_input()
    server_connection = plugin_input.get("server_connection") or {}
    args = plugin_input.get("args") or {}

    scheme = server_connection.get("Scheme") or "http"
    host = server_connection.get("Host") or "127.0.0.1"
    if host == "0.0.0.0":
        host = "127.0.0.1"
    port = server_connection.get("Port") or 9999
    graphql_url = f"{scheme}://{host}:{port}/graphql"

    client = StashClient(
        graphql_url=graphql_url,
        api_key=read_api_key(server_connection.get("Dir")),
        session_cookie=server_connection.get("SessionCookie"),
    )

    dry_run = str(args.get("dry_run", "false")).strip().lower() == "true"
    audience_root_tag, language_root_tag, language_tags, language_tag_ids = load_tag_taxonomy(
        client, dry_run
    )
    galleries = select_target_galleries(client, args)
    targets, skipped = collect_targets(galleries)
    results, failed = process_targets(
        client=client,
        targets=targets,
        audience_root_tag=audience_root_tag,
        language_tag_ids=language_tag_ids,
        language_root_tag=language_root_tag,
        language_tags=language_tags,
        dry_run=dry_run,
    )

    return {
        "error": None,
        "output": {
            "dry_run": dry_run,
            "success": len(results),
            "skipped": skipped,
            "failed": failed,
            "results": results,
        },
    }


def main() -> None:
    try:
        result = run()
    except (HTTPError, URLError, TimeoutError, RuntimeError, ValueError, KeyError) as exc:
        print(json.dumps({"error": str(exc), "output": None}, ensure_ascii=False))
        sys.exit(1)

    output: dict[str, Any] = result.get("output") or {}
    results = output.get("results", [])
    skipped = output.get("skipped", [])
    failed = output.get("failed", [])
    success = output.get("success", len(results))

    emit_info(f"Task finished: {success} succeeded, {len(skipped)} skipped, {len(failed)} failed")
    for item in failed:
        emit_warn(
            f"Failed: gallery_id={item.get('id')} folder={item.get('path')} reason={item.get('reason')}"
        )

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
